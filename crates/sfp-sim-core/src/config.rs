//! Config-driven construction of a `MakerLine`
//! 
//! This module is only compiled when the `config` feature is enabled, so the
//! core stays dependency-minimal (just `rand`) for consumers that build lines
//! programmatically. The config types mirror the domain model one-to-one, so
//! deserialization is a straight map with no translation logic.

use serde::Deserialize;

use crate::line::{MachineState, MakerLine};
use crate::signal::Signal;

/// Top-level line definition, deserialized from TOML.
/// 
/// ```toml
/// name = "Line1"
/// fault_rate = 0.02
/// 
/// [[signals]]
/// name = "HeaterZone1/Temperature"
/// unit = "degC"
/// eu_low = 150.0
/// eu_high = 175.0
/// setpoint = 162.0
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct LineConfig {
    /// Line identity, e.g. "Line1".
    pub name: String,
    /// Per-tick probability (0..1) of a fault event. Optional; defaults to 0.02.
    #[serde(default = "default_fault_rate")]
    pub fault_rate: f64,
    /// The analog sensor channels on this line.
    pub signals: Vec<SignalConfig>,
}

/// One analog signal definition
#[derive(Debug, Clone, Deserialize)]
pub struct SignalConfig {
    pub name: String,
    pub unit: String,
    pub eu_low: f64,
    pub eu_high: f64,
    pub setpoint: f64,
    /// Optional overrides for the dynamics. If omitted, the same defaults as
    /// `Signal::new` apply (responsiveness 0.2, noise = 5% of the EU span).
    #[serde(default)]
    pub responsiveness: Option<f64>,
    #[serde(default)]
    pub noise: Option<f64>,
}

fn default_fault_rate() -> f64 {
    0.02
}

impl From<SignalConfig> for Signal {
    fn from(c: SignalConfig) -> Self {
        // Start from the standard constructor so defaults stay in one place,
        // then apply any optional overrides.
        let mut signal = Signal::new(c.name, c.unit, c.eu_low, c.eu_high, c.setpoint);
        if let Some(r) = c.responsiveness {
            signal.responsiveness = r;
        }
        if let Some(n) = c.noise {
            signal.noise = n;
        }
        signal
    }
}

impl From<LineConfig> for MakerLine {
    fn from(c: LineConfig) -> Self {
        let signals = c.signals.into_iter().map(Signal::from).collect();
        MakerLine {
            name: c.name,
            signals,
            state: MachineState::Running,
            throughput: 0,
            fault_rate: c.fault_rate,
        }
    }
}

impl MakerLine {
    /// Build a line from a TOML string.
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let config: LineConfig = toml::from_str(s).map_err(ConfigError::Parse)?;
        Ok(config.into())
    }

    /// Build a line by reading and parsing a TOML file.
    pub fn from_toml_file(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
        Self::from_toml_str(&text)
    }
}

/// Errors that can occur loading a line config.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be read.
    Io(std::io::Error),
    /// The TOML was malformed or did not match the schema.
    Parse(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "failed to read line config: {e}"),
            ConfigError::Parse(e) => write!(f, "failed to parse line config: {e}"), 
        }
    }
}

impl std::error::Error for ConfigError {}