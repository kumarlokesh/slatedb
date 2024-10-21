use crate::manifest_store::{ManifestStore, StoredManifest};
#[cfg(feature = "aws")]
use log::warn;
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::error::Error;
use std::ops::{RangeBounds, RangeFrom};
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;
use crate::db_state::{Checkpoint, DbState};
use crate::error::SlateDBError;
use crate::error::SlateDBError::InvalidDBState;

/// read-only access to the latest manifest file
pub async fn read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    maybe_id: Option<u64>,
) -> Result<Option<String>, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
    let id_manifest = match maybe_id {
        None => manifest_store.read_latest_manifest().await?,
        Some(id) => manifest_store.read_manifest(id).await?,
    };

    match id_manifest {
        None => Ok(None),
        Some(result) => Ok(Some(serde_json::to_string(&result)?)),
    }
}

pub async fn list_manifests<R: RangeBounds<u64>>(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    range: R,
) -> Result<String, Box<dyn Error>> {
    let manifest_store = ManifestStore::new(path, object_store);
    let manifests = manifest_store.list_manifests(range).await?;
    Ok(serde_json::to_string(&manifests)?)
}

pub async fn create_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    expire_time: Option<SystemTime>,
) -> Result<String, Box<dyn Error>> {
    let manifest_store = Arc::new(ManifestStore::new(path, object_store));
    let maybe_stored_manifest = StoredManifest::load(manifest_store).await?;
    let Some(mut stored_manifest) = maybe_stored_manifest else {
        return Err(Box::new(SlateDBError::ManifestMissing))
    };
    loop {
        let mut core_state = stored_manifest.db_state().clone();
        core_state.checkpoints.push(Checkpoint{
            id: uuid::Uuid::new_v4(),
            manifest_id: stored_manifest.id(),
            expire_time,
        });
        return match stored_manifest.update_db_state(core_state).await {
            Err(SlateDBError::ManifestVersionExists) => {
                stored_manifest.refresh().await?;
                continue;
            },
            Err(e) => Err(Box::new(e)),
            Ok(()) => Ok(String::from("checkpoint created successfully"))
        };
    }
}

pub async fn refresh_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid,
    expire_time: Option<SystemTime>
) -> Result<String, Box<dyn Error>> {
    let manifest_store = Arc::new(ManifestStore::new(path, object_store));
    let maybe_stored_manifest = StoredManifest::load(manifest_store).await?;
    let Some(mut stored_manifest) = maybe_stored_manifest else {
        return Err(Box::new(SlateDBError::ManifestMissing))
    };
    loop {
        let mut core_state = stored_manifest.db_state().clone();
        let mut found = false;
        for checkpoint in core_state.checkpoints.iter_mut() {
            if checkpoint.id == id {
                checkpoint.expire_time = expire_time;
                found = true;
                break;
            }
        }
        if !found {
            return Err(Box::new(InvalidDBState))
        }
        return match stored_manifest.update_db_state(core_state).await {
            Err(SlateDBError::ManifestVersionExists) => {
                stored_manifest.refresh().await?;
                continue;
            },
            Err(e) => Err(Box::new(e)),
            Ok(()) => Ok(String::from("checkpoint refreshed successfully"))
        };
    }
}

pub async fn delete_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid
) -> Result<String, Box<dyn Error>> {
    let manifest_store = Arc::new(ManifestStore::new(path, object_store));
    let maybe_stored_manifest = StoredManifest::load(manifest_store).await?;
    let Some(mut stored_manifest) = maybe_stored_manifest else {
        return Err(Box::new(SlateDBError::ManifestMissing))
    };
    loop {
        let mut core_state = stored_manifest.db_state().clone();
        let checkpoints: Vec<Checkpoint> = core_state.checkpoints.iter()
            .filter(|c| c.id != id)
            .cloned()
            .collect();
        core_state.checkpoints = checkpoints;
        return match stored_manifest.update_db_state(core_state).await {
            Err(SlateDBError::ManifestVersionExists) => {
                stored_manifest.refresh().await?;
                continue;
            },
            Err(e) => Err(Box::new(e)),
            Ok(()) => Ok(String::from("checkpoint deleted successfully"))
        };
    }
}

/// Loads an object store from configured environment variables.
/// The provider is specified using the CLOUD_PROVIDER variable.
/// For specific provider configurations, see the corresponding
/// method documentation:
///
/// | Provider | Value | Documentation |
/// |----------|-------|---------------|
/// | AWS | `aws` | [load_aws] |
pub fn load_object_store_from_env(
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    dotenvy::from_filename(env_file.unwrap_or(String::from(".env"))).ok();

    let provider = &*env::var("CLOUD_PROVIDER")
        .expect("CLOUD_PROVIDER must be set")
        .to_lowercase();

    match provider {
        "local" => load_local(),
        #[cfg(feature = "aws")]
        "aws" => load_aws(),
        #[cfg(feature = "azure")]
        "azure" => load_azure(),
        _ => Err(format!("Unknown CLOUD_PROVIDER: '{}'", provider).into()),
    }
}

/// Loads a local object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | LOCAL_PATH | The path to the local directory where all data will be stored | Yes |
pub fn load_local() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let local_path = env::var("LOCAL_PATH").expect("LOCAL_PATH must be set");
    let lfs = object_store::local::LocalFileSystem::new_with_prefix(local_path)?;
    Ok(Arc::new(lfs) as Arc<dyn ObjectStore>)
}

/// Loads an AWS S3 Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | AWS_ACCESS_KEY_ID | The access key for a role with permissions to access the store | Yes |
/// | AWS_SECRET_ACCESS_KEY | The access key secret for the above ID | Yes |
/// | AWS_SESSION_TOKEN | The session token for the above ID | No |
/// | AWS_BUCKET | The bucket to use within S3 | Yes |
/// | AWS_REGION | The AWS region to use | Yes |
/// | AWS_ENDPOINT | The endpoint to use for S3 (disables https) | No |
/// | AWS_DYNAMODB_TABLE | The DynamoDB table to use for locking | No |
#[cfg(feature = "aws")]
pub fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use object_store::aws::{DynamoCommit, S3ConditionalPut};

    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret =
        env::var("AWS_SECRET_ACCESS_KEY").expect("Expected AWS_SECRET_ACCESS_KEY must be set");
    let session_token = env::var("AWS_SESSION_TOKEN").ok();
    let bucket = env::var("AWS_BUCKET").expect("AWS_BUCKET must be set");
    let region = env::var("AWS_REGION").expect("AWS_REGION must be set");
    let endpoint = env::var("AWS_ENDPOINT").ok();
    let dynamodb_table = env::var("AWS_DYNAMODB_TABLE").ok();
    let builder = object_store::aws::AmazonS3Builder::new()
        .with_access_key_id(key)
        .with_secret_access_key(secret)
        .with_bucket_name(bucket)
        .with_region(region);

    let builder = if let Some(token) = session_token {
        builder.with_token(token)
    } else {
        builder
    };

    let builder = if let Some(dynamodb_table) = dynamodb_table {
        builder.with_conditional_put(S3ConditionalPut::Dynamo(DynamoCommit::new(dynamodb_table)))
    } else {
        warn!(
            "Running without configuring a DynamoDB Table. This is OK when running an admin, \
        but any operations that attempt a CAS will fail."
        );
        builder
    };

    let builder = if let Some(endpoint) = endpoint {
        builder.with_allow_http(true).with_endpoint(endpoint)
    } else {
        builder
    };

    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Loads an Azure Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | AZURE_ACCOUNT | The azure storage account name | Yes |
/// | AZURE_KEY | The azure storage account key| Yes |
/// | AZURE_CONTAINER | The storage container name| Yes |
#[cfg(feature = "azure")]
pub fn load_azure() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let account = env::var("AZURE_ACCOUNT").expect("AZURE_ACCOUNT must be set");
    let key = env::var("AZURE_KEY").expect("AZURE_KEY must be set");
    let container = env::var("AZURE_CONTAINER").expect("AZURE_CONTAINER must be set");
    let builder = object_store::azure::MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_access_key(key)
        .with_container_name(container);
    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}
