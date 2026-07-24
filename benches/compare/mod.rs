//! Modular absolute multi-client MQTT compare harness.
//!
//! See `benches/README.md` and crate docs for env vars / JSONL output.

pub mod clients;
pub mod fixtures;
pub mod harness;
pub mod pubchild;
pub mod report;
pub mod run;
pub mod scenario;

pub use run::execute_matrix;
pub use scenario::build_matrix;
