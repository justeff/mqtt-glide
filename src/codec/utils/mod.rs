//! Shared wire helpers for the internal MQTT 5 codec.
//!
//! Thin facade over `framing` (fixed header, Remaining Length) and `primitives` (§1.5 data
//! types). The `ensure!` macro is defined here so v5 packet modules can use it via
//! `#[macro_use] mod utils` in `codec/mod.rs`. Does **not** own packet semantics, session
//! policy, or tokio framing — those live in `codec/v5` and the connection layers above.

/// Helper macro for early error return on condition failure
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err($fmt, $($arg)+);
        }
    };
}

mod framing;
mod primitives;
mod reader;

pub(crate) use framing::*;
pub(crate) use primitives::*;
pub(crate) use reader::WireReader;
