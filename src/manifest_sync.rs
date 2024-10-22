use std::sync::Arc;
use parking_lot::RwLock;
use crate::db_state::{Checkpoint, CoreDbState, DbState};
use crate::error::SlateDBError;
use crate::manifest_store::WritableManifest;

struct ManifestSyncTask {
    manifest: Box<dyn WritableManifest>,
    state: Arc<RwLock<DbState>>,
}

impl ManifestSyncTask {
    fn new(manifest: Box<dyn WritableManifest>, state: Arc<RwLock<DbState>>) -> Self {
        Self{
            manifest,
            state,
        }
    }

    fn reestablish_checkpoint(&self, refreshed_state: &CoreDbState) -> bool {
        let state = &self.state.read().state().core;
        if state.l0_last_compacted != refreshed_state.l0_last_compacted {
            return true;
        }
        if state.compacted != refreshed_state.compacted {
            return true;
        }
        return false;
    }

    async fn refresh(&mut self) -> Result<(), SlateDBError> {
        loop {
            // refresh the manifest
            self.manifest.refresh()?;
            let state = self.manifest.db_state()?;
            if !self.reestablish_checkpoint(state) {
                return Ok(())
            }
            let mut updated = {
                let mut rguard = self.state.read();
                rguard.merge_db_state(state)
                // todo: this thing needs to return a checkpoint acceptor in the case that
                //       a merge is required
            };
            let manifest_id = self.manifest.id();
            let checkpoint = Checkpoint {
                id: uuid::Uuid::new_v4(),
                manifest_id,
                expire_time: None
            };
            updated.checkpoints.push(checkpoint);
            match self.manifest.update_db_state(updated).await {
                Err(SlateDBError::ManifestVersionExists) => {},
                Err(err) => return Err(err),
                Ok(()) => {
                    let mut wguard = self.state.write();
                    // problem: this update is for a checkpoint, but the checkpoint may not contain
                    // the latest updates to the coredbstate - e.g. the db may have flushed more
                    // memtables at this point
                    wguard.refresh_db_state(&updated);
                    return Ok(())
                }
            }
        }
    }
}