use crate::args::{parse_args, CliArgs, CliCommands};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::admin;
use slatedb::admin::{list_manifests, read_manifest, create_checkpoint, refresh_checkpoint, delete_checkpoint};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    match args.command {
        CliCommands::ReadManifest { id } => exec_read_manifest(&path, object_store, id).await?,
        CliCommands::ListManifests { start, end } => {
            exec_list_manifest(&path, object_store, start, end).await?
        },
        CliCommands::CreateCheckpoint { lifetime }  => {
            exec_create_checkpoint(&path, object_store, lifetime).await?
        },
        CliCommands::RefreshCheckpoint { id, lifetime } => {}
        CliCommands::DeleteCheckpoint { id } => {}
    }

    Ok(())
}

async fn exec_read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    match read_manifest(path, object_store, id).await? {
        None => {
            println!("No manifest file found.")
        }
        Some(manifest) => {
            println!("{}", manifest);
        }
    }
    Ok(())
}

async fn exec_list_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let range = match (start, end) {
        (Some(s), Some(e)) => s..e,
        (Some(s), None) => s..u64::MAX,
        (None, Some(e)) => u64::MIN..e,
        _ => u64::MIN..u64::MAX,
    };

    Ok(println!(
        "{}",
        list_manifests(path, object_store, range).await?
    ))
}

async fn exec_create_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    lifetime: Option<Duration>,
) -> Result<(), Box<dyn Error>> {
    let expire_time = lifetime.map(|l| SystemTime::now() + l);
    Ok(println!(
        "{}",
        create_checkpoint(path, object_store, expire_time).await?
    ))
}

async fn exec_refresh_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid,
    lifetime: Option<Duration>
) -> Result<(), Box<dyn Error>> {
    let expire_time = lifetime.map(|l| SystemTime::now() + l);
    Ok(println!(
        "{}",
        refresh_checkpoint(path, object_store, id, expire_time).await?
    ))
}

async fn exec_delete_checkpoint(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
    id: Uuid
) -> Result<(), Box<dyn Error>> {
    Ok(println!(
        "{}",
        delete_checkpoint(path, object_store, id).await?
    ))
}