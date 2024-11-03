use bytes::Bytes;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info};
use ulid::Ulid;
use uuid::Uuid;
use SsTableId::{Compacted, Wal};

use crate::config::CompressionCodec;
use crate::error::SlateDBError;
use crate::mem_table::{ImmutableMemtable, ImmutableWal, KVTable, WritableKVTable};

#[derive(Clone, PartialEq, Serialize)]
pub(crate) struct SsTableHandle {
    pub id: SsTableId,
    pub info: SsTableInfo,
}

impl SsTableHandle {
    pub(crate) fn new(id: SsTableId, info: SsTableInfo) -> Self {
        SsTableHandle { id, info }
    }

    pub(crate) fn range_covers_key(&self, key: &[u8]) -> bool {
        if let Some(first_key) = self.info.first_key.as_ref() {
            return key >= first_key;
        }
        false
    }

    pub(crate) fn estimate_size(&self) -> u64 {
        // this is a hacky estimate of the sst size since we don't have it stored anywhere
        // right now. Just use the index's offset and add the index length. Since the index
        // is the last thing we put in the SST before the info footer, this should be a good
        // estimate for now.
        self.info.index_offset + self.info.index_len
    }
}

impl AsRef<SsTableHandle> for SsTableHandle {
    fn as_ref(&self) -> &SsTableHandle {
        self
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub(crate) struct Checkpoint {
    pub(crate) id: Uuid,
    pub(crate) manifest_id: u64,
    pub(crate) expire_time: Option<SystemTime>,
}

#[derive(Clone, PartialEq, Debug, Hash, Eq, Copy, Serialize)]
pub(crate) enum SsTableId {
    Wal(u64),
    Compacted(Ulid),
}

impl SsTableId {
    #[allow(clippy::panic)]
    pub(crate) fn unwrap_wal_id(&self) -> u64 {
        match self {
            Wal(wal_id) => *wal_id,
            Compacted(_) => panic!("found compacted id when unwrapping WAL ID"),
        }
    }

    #[allow(clippy::panic)]
    pub(crate) fn unwrap_compacted_id(&self) -> Ulid {
        match self {
            Wal(_) => panic!("found WAL id when unwrapping compacted ID"),
            Compacted(ulid) => *ulid,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum RowFeature {
    Flags,
    Timestamp,
    ExpireAtTs,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct SsTableInfo {
    pub(crate) first_key: Option<Bytes>,
    pub(crate) index_offset: u64,
    pub(crate) index_len: u64,
    pub(crate) filter_offset: u64,
    pub(crate) filter_len: u64,
    pub(crate) compression_codec: Option<CompressionCodec>,
    pub(crate) row_features: Vec<RowFeature>,
}

pub(crate) trait SsTableInfoCodec: Send + Sync {
    fn encode(&self, manifest: &SsTableInfo) -> Bytes;

    fn decode(&self, bytes: &Bytes) -> Result<SsTableInfo, SlateDBError>;

    fn clone_box(&self) -> Box<dyn SsTableInfoCodec>;
}

/// Implement Clone for Box<dyn SsTableInfoCodec> by delegating to the clone_box method.
/// This is the idiomatic way to clone trait objects in Rust. It is also the only way to
/// clone trait objects without knowing the concrete type.
impl Clone for Box<dyn SsTableInfoCodec> {
    fn clone(&self) -> Self {
        self.as_ref().clone_box()
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub(crate) struct SortedRun {
    pub(crate) id: u32,
    pub(crate) ssts: Vec<SsTableHandle>,
}

impl SortedRun {
    pub(crate) fn estimate_size(&self) -> u64 {
        self.ssts.iter().map(|sst| sst.estimate_size()).sum()
    }

    pub(crate) fn find_sst_with_range_covering_key_idx(&self, key: &[u8]) -> Option<usize> {
        // returns the sst after the one whose range includes the key
        let first_sst = self.ssts.partition_point(|sst| {
            sst.info
                .first_key
                .as_ref()
                .expect("sst must have first key")
                <= key
        });
        if first_sst > 0 {
            return Some(first_sst - 1);
        }
        // all ssts have a range greater than the key
        None
    }

    pub(crate) fn find_sst_with_range_covering_key(&self, key: &[u8]) -> Option<&SsTableHandle> {
        self.find_sst_with_range_covering_key_idx(key)
            .map(|idx| &self.ssts[idx])
    }
}





// represents the core db state that we persist in the manifest
#[derive(Clone, PartialEq, Serialize)]
pub(crate) struct CoreDbState {
    pub(crate) l0_last_compacted: Option<Ulid>,
    pub(crate) l0: VecDeque<SsTableHandle>,
    pub(crate) compacted: Vec<SortedRun>,
    pub(crate) next_wal_sst_id: u64,
    pub(crate) last_compacted_wal_sst_id: u64,
    pub(crate) checkpoints: Vec<Checkpoint>,
}

impl CoreDbState {
    pub(crate) fn new() -> Self {
        Self {
            l0_last_compacted: None,
            l0: VecDeque::new(),
            compacted: vec![],
            next_wal_sst_id: 1,
            last_compacted_wal_sst_id: 0,
            checkpoints: vec![],
        }
    }

    pub(crate) fn log_db_runs(&self) {
        let l0s: Vec<_> = self.l0.iter().map(|l0| l0.estimate_size()).collect();
        let compacted: Vec<_> = self
            .compacted
            .iter()
            .map(|sr| (sr.id, sr.estimate_size()))
            .collect();
        debug!("DB Levels:");
        debug!("-----------------");
        debug!("{:?}", l0s);
        debug!("{:?}", compacted);
        debug!("-----------------");
    }
}

// represents the state that is mutated by creating a new copy with the mutations
#[derive(Clone)]
pub(crate) struct COWDbState {
    pub(crate) imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    pub(crate) imm_wal: VecDeque<Arc<ImmutableWal>>,
    pub(crate) core: CoreDbState,
}

// represents a read-snapshot of the current db state
#[derive(Clone)]
pub(crate) struct DbStateSnapshot {
    pub(crate) memtable: Arc<KVTable>,
    pub(crate) wal: Arc<KVTable>,
    pub(crate) state: Arc<COWDbState>,
}

pub(crate) struct DbState {
    memtable: WritableKVTable,
    wal: WritableKVTable,
    state: Arc<COWDbState>,
    next_wal_sst_id: u64,
    coredb_state_update_log: VecDeque<StateMutationEntry>,
    next_seq_num: u64,
}

enum StateMutation {
    SetNextWalSstId{ next_wal_sst_id: u64 },
    MoveImmMemtableToL0 { imm_memtable: Arc<ImmutableMemtable>, sst_handle: SsTableHandle }
}

struct StateMutationEntry {
    mutation: StateMutation,
    seq_num: u64,
}

struct CoreDbStateUpdate {
    state: CoreDbState,
    seq_num: u64,
}

impl DbState {
    pub fn new(core_db_state: CoreDbState) -> Self {
        let next_wal_sst_id = core_db_state.next_wal_sst_id;
        Self {
            memtable: WritableKVTable::new(),
            wal: WritableKVTable::new(),
            state: Arc::new(COWDbState {
                imm_memtable: VecDeque::new(),
                imm_wal: VecDeque::new(),
                core: core_db_state,
            }),
            coredb_state_update_log: VecDeque::new(),
            next_seq_num: 1,
            next_wal_sst_id,
        }
    }

    pub fn state(&self) -> Arc<COWDbState> {
        self.state.clone()
    }

    pub fn snapshot(&self) -> DbStateSnapshot {
        DbStateSnapshot {
            memtable: self.memtable.table().clone(),
            wal: self.wal.table().clone(),
            state: self.state.clone(),
        }
    }

    pub fn last_written_wal_id(&self) -> u64 {
        // todo: fix me - this is not the last written wal id
        assert!(self.next_wal_sst_id > 0);
        self.next_wal_sst_id - 1
    }

    // mutations

    fn log_mutation(&mut self, mutation: StateMutation) {
        let seq_num = self.next_seq_num;
        self.next_seq_num += 1;
        self.coredb_state_update_log.push_back(StateMutationEntry{
            seq_num,
            mutation,
        })
    }

    pub fn wal(&mut self) -> &mut WritableKVTable {
        &mut self.wal
    }

    pub fn memtable(&mut self) -> &mut WritableKVTable {
        &mut self.memtable
    }

    fn state_copy(&self) -> COWDbState {
        self.state.as_ref().clone()
    }

    fn update_state(&mut self, state: COWDbState) {
        self.state = Arc::new(state);
    }

    fn apply_mutation_to_coredb_state(mutation: &StateMutation, state: &mut CoreDbState) {
        match mutation {
            StateMutation::SetNextWalSstId { next_wal_sst_id } => {
                state.next_wal_sst_id = *next_wal_sst_id;
            }
            StateMutation::MoveImmMemtableToL0 { imm_memtable, sst_handle } => {
                state.l0.push_front(sst_handle.clone());
                state.last_compacted_wal_sst_id = imm_memtable.last_wal_id();
            }
        }
    }

    fn apply_mutation(&mut self, mutation: StateMutation) {
        let mut state_copy = self.state_copy();
        Self::apply_mutation_to_coredb_state(&mutation, &mut state_copy.core);
        match mutation {
            StateMutation::MoveImmMemtableToL0 { imm_memtable, sst_handle } => {
                let popped = state_copy.imm_memtable.pop_back().expect("expected memtable");
                assert!(Arc::ptr_eq(&imm_memtable, &popped))
            }
            _ => {}
        }
    }

    fn apply_log_to_core_db_state(&self) -> CoreDbState {
        let mut state = self.state().core.clone();
        for mutation in self.coredb_state_update_log.iter() {
            Self::apply_mutation_to_coredb_state(&mutation.mutation, &mut state);
        }
        state
    }

    fn apply_log(&mut self, until_seq_num: u64) {
        loop {
            let Some(front) = self.coredb_state_update_log.front() else {
                break;
            };
            if front.seq_num > until_seq_num {
                break;
            }
            let front = self.coredb_state_update_log.pop_front().expect("unreachable");
            self.apply_mutation(front.mutation);
        }
    }

    pub fn freeze_memtable(&mut self, wal_id: u64) {
        let old_memtable = std::mem::replace(&mut self.memtable, WritableKVTable::new());
        let mut state = self.state_copy();
        state
            .imm_memtable
            .push_front(Arc::new(ImmutableMemtable::new(old_memtable, wal_id)));
        self.update_state(state);
    }

    pub fn freeze_wal(&mut self) -> Option<u64> {
        if self.wal.table().is_empty() {
            return None;
        }
        let old_wal = std::mem::replace(&mut self.wal, WritableKVTable::new());
        let mut state = self.state_copy();
        let imm_wal = Arc::new(ImmutableWal::new(state.core.next_wal_sst_id, old_wal));
        let id = imm_wal.id();
        state.imm_wal.push_front(imm_wal);
        self.update_state(state);
        self.next_wal_sst_id += 1;
        Some(id)
    }

    pub fn pop_imm_wal(&mut self) {
        let mut state = self.state_copy();
        let imm_wal = state.imm_wal.pop_back().expect("expected wal");
        let wal_id = imm_wal.id();
        self.log_mutation(StateMutation::SetNextWalSstId { next_wal_sst_id:  wal_id + 1});
        self.update_state(state);
    }

    pub fn move_imm_memtable_to_l0(
        &mut self,
        imm_memtable: Arc<ImmutableMemtable>,
        sst_handle: SsTableHandle,
    ) {
        self.log_mutation(StateMutation::MoveImmMemtableToL0 {imm_memtable, sst_handle});
    }

    pub fn increment_next_wal_id(&mut self) {
        self.next_wal_sst_id += 1;
        self.log_mutation(StateMutation::SetNextWalSstId { next_wal_sst_id: self.next_wal_sst_id });
    }

    pub fn prepare_update(&self) -> Option<CoreDbStateUpdate> {

    }

    pub fn merge_from_remote(&mut self, remote_state: &CoreDbState) -> Option<CoreDbStateUpdate> {
        // todo: if remote state is different, establish new checkpoint and remove
        //       current checkpoint

    }

    pub fn on_update_applied(&mut self, update: CoreDbStateUpdate) {

    }

    pub fn refresh_db_state(&mut self, compactor_state: &CoreDbState) {
        // copy over L0 up to l0_last_compacted
        let mut state = self.state_copy();
        state.core = self.merge_db_state(compactor_state);
        self.update_state(state)
    }

    // TODO: move to reader section
    pub fn merge_db_state(&self, compactor_state: &CoreDbState) -> CoreDbState {
        // copy over L0 up to l0_last_compacted
        let l0_last_compacted = &compactor_state.l0_last_compacted;
        let mut new_l0 = VecDeque::new();
        for sst in self.state.core.l0.iter() {
            if let Some(l0_last_compacted) = l0_last_compacted {
                if sst.id.unwrap_compacted_id() == *l0_last_compacted {
                    break;
                }
            }
            new_l0.push_back(sst.clone());
        }
        let compacted = compactor_state.compacted.clone();
        let mut core = self.state.core.clone();
        core.l0_last_compacted.clone_from(l0_last_compacted);
        core.l0 = new_l0;
        core.compacted = compacted;
        core
    }
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use crate::db_state::{
        CoreDbState, DbState, RowFeature, SsTableHandle, SsTableId, SsTableInfo,
    };

    #[test]
    fn test_should_refresh_db_state_with_l0s_up_to_last_compacted() {
        // given:
        let mut db_state = DbState::new(CoreDbState::new());
        add_l0s_to_dbstate(&mut db_state, 4);
        // mimic the compactor popping off l0s
        let mut compactor_state = db_state.state.core.clone();
        let last_compacted = compactor_state.l0.pop_back().unwrap();
        compactor_state.l0_last_compacted = Some(last_compacted.id.unwrap_compacted_id());

        // when:
        db_state.refresh_db_state(&compactor_state);

        // then:
        let expected: Vec<SsTableId> = compactor_state.l0.iter().map(|l0| l0.id).collect();
        let merged: Vec<SsTableId> = db_state.state.core.l0.iter().map(|l0| l0.id).collect();
        assert_eq!(expected, merged);
    }

    #[test]
    fn test_should_refresh_db_state_with_all_l0s_if_none_compacted() {
        // given:
        let mut db_state = DbState::new(CoreDbState::new());
        add_l0s_to_dbstate(&mut db_state, 4);
        let l0s = db_state.state.core.l0.clone();

        // when:
        db_state.refresh_db_state(&CoreDbState::new());

        // then:
        let expected: Vec<SsTableId> = l0s.iter().map(|l0| l0.id).collect();
        let merged: Vec<SsTableId> = db_state.state.core.l0.iter().map(|l0| l0.id).collect();
        assert_eq!(expected, merged);
    }

    fn add_l0s_to_dbstate(db_state: &mut DbState, n: u32) {
        let dummy_info = create_sst_info();
        for i in 0..n {
            db_state.freeze_memtable(i as u64);
            let imm = db_state.state.imm_memtable.back().unwrap().clone();
            let handle = SsTableHandle::new(SsTableId::Compacted(Ulid::new()), dummy_info.clone());
            db_state.move_imm_memtable_to_l0(imm, handle);
        }
    }

    fn create_sst_info() -> SsTableInfo {
        SsTableInfo {
            first_key: None,
            index_offset: 0,
            index_len: 0,
            filter_offset: 0,
            filter_len: 0,
            compression_codec: None,
            row_features: vec![RowFeature::Flags],
        }
    }
}
