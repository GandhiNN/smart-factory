//! sfp-sim-core: a protocol-agnostic simulation of a Smoke-Free Product
//! manufacturing line.
//! 
//! This crate models *process and sensors only*. It knows nothing about
//! OPC-UA, MQTT, Kafka, or any transport - those live in higher layers and
//! read from the types defined here. Keeping the domain free of protocol
//! concerns is the central design decision of the whole project.

pub mod line;
pub mod quality;
pub mod signal;

// Re-export the main types at the crate root so downstream crates can write
// `use sfp_sim_core::{MakerLine, Signal, Quality, MachineState};`
pub use line::{MachineState, MakerLine};
pub use quality::Quality;
pub use signal::Signal;