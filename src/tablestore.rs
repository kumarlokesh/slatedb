use crate::blob::ReadOnlyBlob;
use crate::block::Block;
use crate::error::SlateDBError;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::{ManifestV1Owned, SsTableInfoOwned};
use crate::sst::{EncodedSsTable, EncodedSsTableBuilder, SsTableFormat};
use bytes::{BufMut, Bytes};
use fail_parallel::{fail_point, FailPointRegistry};
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

pub struct TableStore {
    object_store: Arc<dyn ObjectStore>,
    sst_format: SsTableFormat,
    root_path: Path,
    wal_path: &'static str,
    compacted_path: &'static str,
    manifest_path: &'static str,
    fp_registry: Arc<FailPointRegistry>,
}

struct ReadOnlyObject {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ReadOnlyBlob for ReadOnlyObject {
    async fn len(&self) -> Result<usize, SlateDBError> {
        let object_metadata = self
            .object_store
            .head(&self.path)
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(object_metadata.size)
    }

    async fn read_range(&self, range: Range<usize>) -> Result<Bytes, SlateDBError> {
        self.object_store
            .get_range(&self.path, range)
            .await
            .map_err(SlateDBError::ObjectStoreError)
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        let file = self.object_store.get(&self.path).await?;
        file.bytes().await.map_err(SlateDBError::ObjectStoreError)
    }
}

#[derive(Clone, PartialEq, Debug, Hash, Eq)]
pub enum SsTableId {
    Wal(u64),
    Compacted(Ulid),
}

impl SsTableId {
    #[allow(clippy::panic)]
    pub(crate) fn unwrap_compacted_id(&self) -> Ulid {
        match self {
            SsTableId::Wal(_) => panic!("found WAL id when unwrapping compacted ID"),
            SsTableId::Compacted(ulid) => *ulid,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct SSTableHandle {
    pub id: SsTableId,
    pub info: SsTableInfoOwned,
    // TODO: we stash the filter in the handle for now, as a way to cache it so that
    //       the db doesn't need to reload it for each read. Once we've put in a proper
    //       cache, we should instead cache the filter block in the cache and get rid
    //       of this reference.
    //       https://github.com/slatedb/slatedb/issues/89
    filter: Option<Arc<BloomFilter>>,
}

impl TableStore {
    #[allow(dead_code)]
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: Path,
    ) -> Self {
        Self::new_with_fp_registry(
            object_store,
            sst_format,
            root_path,
            Arc::new(FailPointRegistry::new()),
        )
    }

    pub fn new_with_fp_registry(
        object_store: Arc<dyn ObjectStore>,
        sst_format: SsTableFormat,
        root_path: Path,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Self {
        Self {
            object_store: object_store.clone(),
            sst_format,
            root_path,
            wal_path: "wal",
            compacted_path: "compacted",
            manifest_path: "manifest",
            fp_registry,
        }
    }

    pub(crate) async fn open_latest_manifest(
        &self,
    ) -> Result<Option<ManifestV1Owned>, SlateDBError> {
        let manifest_path = &Path::from(format!("{}/{}/", &self.root_path, self.manifest_path));
        let mut files_stream = self.object_store.list(Some(manifest_path));
        let mut manifest_file_path: Option<Path> = None;

        while let Some(file) = match files_stream.next().await.transpose() {
            Ok(file) => file,
            Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
        } {
            match self.parse_id(&file.location, "manifest") {
                Ok(_) => {
                    manifest_file_path = match manifest_file_path {
                        Some(path) => Some(if path < file.location {
                            file.location
                        } else {
                            path
                        }),
                        None => Some(file.location.clone()),
                    }
                }
                Err(_) => continue,
            }
        }

        if let Some(resolved_manifest_file_path) = manifest_file_path {
            let manifest_bytes = match self.object_store.get(&resolved_manifest_file_path).await {
                Ok(manifest) => match manifest.bytes().await {
                    Ok(bytes) => bytes,
                    Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
                },
                Err(e) => return Err(SlateDBError::ObjectStoreError(e)),
            };

            ManifestV1Owned::new(manifest_bytes.clone())
                .map(Some)
                .map_err(SlateDBError::InvalidFlatbuffer)
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn get_wal_sst_list(
        &self,
        wal_id_last_compacted: u64,
    ) -> Result<Vec<u64>, SlateDBError> {
        let mut wal_list: Vec<u64> = Vec::new();
        let wal_path = &Path::from(format!("{}/{}/", &self.root_path, self.wal_path));
        let mut files_stream = self.object_store.list(Some(wal_path));

        while let Some(file) = files_stream.next().await.transpose()? {
            match self.parse_id(&file.location, "sst") {
                Ok(wal_id) => {
                    if wal_id > wal_id_last_compacted {
                        wal_list.push(wal_id);
                    }
                }
                Err(_) => continue,
            }
        }

        wal_list.sort();
        Ok(wal_list)
    }

    pub(crate) async fn write_manifest(
        &self,
        manifest: &ManifestV1Owned,
    ) -> Result<(), SlateDBError> {
        let manifest_path = &Path::from(format!(
            "{}/{}/{:020}.{}",
            &self.root_path,
            self.manifest_path,
            manifest.borrow().manifest_id(),
            self.manifest_path
        ));

        self.object_store
            .put(manifest_path, Bytes::copy_from_slice(manifest.data()))
            .await
            .map_err(SlateDBError::ObjectStoreError)?;

        Ok(())
    }

    pub(crate) fn table_writer(&self, id: SsTableId) -> EncodedSsTableWriter {
        let path = self.path(&id);
        EncodedSsTableWriter {
            id,
            builder: self.sst_format.table_builder(),
            writer: BufWriter::new(self.object_store.clone(), path),
        }
    }

    pub(crate) fn table_builder(&self) -> EncodedSsTableBuilder {
        self.sst_format.table_builder()
    }

    pub(crate) async fn write_sst(
        &self,
        id: &SsTableId,
        encoded_sst: EncodedSsTable,
    ) -> Result<SSTableHandle, SlateDBError> {
        fail_point!(
            self.fp_registry.clone(),
            "write-wal-sst-io-error",
            matches!(id, SsTableId::Wal(_)),
            |_| Result::Err(SlateDBError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "oops"
            )))
        );
        fail_point!(
            self.fp_registry.clone(),
            "write-compacted-sst-io-error",
            matches!(id, SsTableId::Compacted(_)),
            |_| Result::Err(SlateDBError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "oops"
            )))
        );

        let path = self.path(id);
        let total_size = encoded_sst
            .unconsumed_blocks
            .iter()
            .map(|chunk| chunk.len())
            .sum();
        let mut data = Vec::<u8>::with_capacity(total_size);
        for chunk in encoded_sst.unconsumed_blocks {
            data.put_slice(chunk.as_ref())
        }
        self.object_store
            .put(&path, Bytes::from(data))
            .await
            .map_err(SlateDBError::ObjectStoreError)?;
        Ok(SSTableHandle {
            id: id.clone(),
            info: encoded_sst.info,
            filter: encoded_sst.filter,
        })
    }

    // TODO: once we move filters into cache, this method should no longer be async
    //       https://github.com/slatedb/slatedb/issues/89
    pub(crate) async fn open_compacted_sst(
        &self,
        id: SsTableId,
        info: SsTableInfoOwned,
    ) -> Result<SSTableHandle, SlateDBError> {
        let path = self.path(&id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let filter = self.sst_format.read_filter(&info, &obj).await?;
        Ok(SSTableHandle {
            id: id.clone(),
            info,
            filter,
        })
    }

    // todo: clean up the warning suppression when we start using open_sst outside tests
    #[allow(dead_code)]
    pub(crate) async fn open_sst(&self, id: &SsTableId) -> Result<SSTableHandle, SlateDBError> {
        let path = self.path(id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        let info = self.sst_format.read_info(&obj).await?;
        let filter = self.sst_format.read_filter(&info, &obj).await?;
        Ok(SSTableHandle {
            id: id.clone(),
            info,
            filter,
        })
    }

    pub(crate) async fn read_filter(
        &self,
        handle: &SSTableHandle,
    ) -> Result<Option<Arc<BloomFilter>>, SlateDBError> {
        Ok(handle.filter.clone())
    }

    pub(crate) async fn read_blocks(
        &self,
        handle: &SSTableHandle,
        blocks: Range<usize>,
    ) -> Result<VecDeque<Block>, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format
            .read_blocks(&handle.info, blocks, &obj)
            .await
    }

    pub(crate) async fn read_block(
        &self,
        handle: &SSTableHandle,
        block: usize,
    ) -> Result<Block, SlateDBError> {
        let path = self.path(&handle.id);
        let obj = ReadOnlyObject {
            object_store: self.object_store.clone(),
            path,
        };
        self.sst_format.read_block(&handle.info, block, &obj).await
    }

    fn path(&self, id: &SsTableId) -> Path {
        match id {
            SsTableId::Wal(id) => Path::from(format!(
                "{}/{}/{:020}.sst",
                &self.root_path, self.wal_path, id
            )),
            SsTableId::Compacted(ulid) => Path::from(format!(
                "{}/{}/{}.sst",
                &self.root_path,
                self.compacted_path,
                ulid.to_string()
            )),
        }
    }

    fn parse_id(&self, path: &Path, expected_extension: &str) -> Result<u64, SlateDBError> {
        match path.extension() {
            Some(ext) if ext == expected_extension => path
                .filename()
                .expect("invalid wal file")
                .split('.')
                .next()
                .ok_or_else(|| SlateDBError::InvalidDBState)?
                .parse()
                .map_err(|_| SlateDBError::InvalidDBState),
            _ => Err(SlateDBError::InvalidDBState),
        }
    }
}

pub(crate) struct EncodedSsTableWriter<'a> {
    id: SsTableId,
    builder: EncodedSsTableBuilder<'a>,
    writer: BufWriter,
}

impl<'a> EncodedSsTableWriter<'a> {
    pub async fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SlateDBError> {
        self.builder.add(key, value)?;
        self.drain_blocks().await
    }

    pub async fn close(mut self) -> Result<SSTableHandle, SlateDBError> {
        let mut encoded_sst = self.builder.build()?;
        while let Some(block) = encoded_sst.unconsumed_blocks.pop_front() {
            self.writer.write_all(block.as_ref()).await?;
        }
        self.writer.shutdown().await?;
        Ok(SSTableHandle {
            id: self.id.clone(),
            info: encoded_sst.info,
            filter: encoded_sst.filter,
        })
    }

    async fn drain_blocks(&mut self) -> Result<(), SlateDBError> {
        while let Some(block) = self.builder.next_block() {
            self.writer.write_all(block.as_ref()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::sst::SsTableFormat;
    use crate::sst_iter::SstIterator;
    use crate::tablestore::{SsTableId, TableStore};
    use crate::test_utils::assert_iterator;
    use crate::types::ValueDeletable;
    use bytes::Bytes;
    use object_store::path::Path;
    use std::sync::Arc;
    use ulid::Ulid;

    const ROOT: &str = "/root";

    #[tokio::test]
    async fn test_sst_writer_should_write_sst() {
        // given:
        let os = Arc::new(object_store::memory::InMemory::new());
        let format = SsTableFormat::new(32, 1);
        let ts = Arc::new(TableStore::new(os.clone(), format, Path::from(ROOT)));
        let id = SsTableId::Compacted(Ulid::new());

        // when:
        let mut writer = ts.table_writer(id);
        writer.add(&[b'a'; 16], Some(&[1u8; 16])).await.unwrap();
        writer.add(&[b'b'; 16], Some(&[2u8; 16])).await.unwrap();
        writer.add(&[b'c'; 16], None).await.unwrap();
        writer.add(&[b'd'; 16], Some(&[4u8; 16])).await.unwrap();
        let sst = writer.close().await.unwrap();

        // then:
        let mut iter = SstIterator::new(&sst, ts.clone(), 1, 1);
        assert_iterator(
            &mut iter,
            &[
                (
                    &[b'a'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[1u8; 16])),
                ),
                (
                    &[b'b'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[2u8; 16])),
                ),
                (&[b'c'; 16], ValueDeletable::Tombstone),
                (
                    &[b'd'; 16],
                    ValueDeletable::Value(Bytes::copy_from_slice(&[4u8; 16])),
                ),
            ],
        )
        .await;
    }
}
