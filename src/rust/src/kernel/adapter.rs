//! Isolation layer for concrete Delta Kernel APIs.
//!
//! Production snapshot and CDF scans extend this module. No other package
//! module should depend on concrete Delta Kernel types.

use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::Snapshot;

pub(crate) fn smoke() -> Result<&'static str, String> {
    let store = Arc::new(InMemory::new());
    let _engine = DefaultEngineBuilder::new(store).build();
    let _snapshot_builder = Snapshot::builder_for("memory:///delta-sharing-r-smoke");

    Ok("Delta Kernel default engine and snapshot builder constructed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_kernel_default_engine_constructs() {
        let message = smoke().expect("kernel/default-engine smoke path must construct");
        assert!(message.contains("Delta Kernel"));
    }
}
