use std::path::Path;
use std::sync::OnceLock;

use lance::Dataset;
use lance_index::scalar::ScalarIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
use tokio::sync::Mutex;
use tracing::debug;

use crate::catalog::schema_ir::NodeTypeDef;
use crate::error::{NanoError, Result};

const SCALAR_INDEX_SUFFIX: &str = "_btree_idx";
static SCALAR_INDEX_BUILD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub fn scalar_index_name(type_id: u32, property: &str) -> String {
    format!("nano_{:08x}_{}{}", type_id, property, SCALAR_INDEX_SUFFIX)
}

pub(crate) async fn rebuild_node_scalar_indexes(
    dataset_path: &Path,
    node_def: &NodeTypeDef,
) -> Result<()> {
    let indexed_props: Vec<&str> = node_def
        .properties
        .iter()
        .filter(|prop| prop.index)
        .map(|prop| prop.name.as_str())
        .collect();
    if indexed_props.is_empty() {
        return Ok(());
    }

    // Lance scalar index builds use a shared memory pool. Building multiple indexes
    // concurrently across tests/process tasks can exhaust that pool for tiny workloads.
    // Serialize builds to keep resource usage predictable.
    let build_lock = SCALAR_INDEX_BUILD_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = build_lock.lock().await;

    let uri = dataset_path.to_string_lossy().to_string();
    let mut dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;

    let index_params = ScalarIndexParams::default();
    for prop in indexed_props {
        let index_name = scalar_index_name(node_def.type_id, prop);
        dataset
            .create_index(
                &[prop],
                IndexType::Scalar,
                Some(index_name.clone()),
                &index_params,
                true,
            )
            .await
            .map_err(|e| {
                NanoError::Lance(format!(
                    "create scalar index `{}` on {}.{} failed: {}",
                    index_name, node_def.name, prop, e
                ))
            })?;
        debug!(
            node_type = %node_def.name,
            property = %prop,
            index_name = %index_name,
            "created/replaced scalar index"
        );
    }

    Ok(())
}
