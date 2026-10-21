//! The translation boundary between our domain model (`sfp-sim-core`) and the
//! OPC-UA protocol. This is the ONLY place that knows how a process `Quality`
//! maps onto an OPC-UA `StatusCode`. Keeping it isolated means the core sim
//! never learns what a `StatusCode` is, and the rest of the server never
//! open-codes the mapping.

use opcua::types::StatusCode;
use sfp_sim_core::Quality;

/// Translate a domain `Quality` into the OPC-UA `StatusCode` a client expects.
/// 
/// - `Good`        -> `Good` (0x0): trust the value.
/// - `Uncertain`   -> `UncertainLastUsableValue`: value is stale/suspect but
/// not meaningless; clients may use it with caution
/// - `Bad`         -> `BadDeviceFailure`: the sensor/device failed; the value
/// must not be trusted. This is the canonical code real devices emit.
pub fn quality_to_status(quality: Quality) -> StatusCode {
    match quality {
        Quality::Good => StatusCode::Good,
        Quality::Uncertain => StatusCode::UncertainLastUsableValue,
        Quality::Bad => StatusCode::BadDeviceFailure,
    }
}
