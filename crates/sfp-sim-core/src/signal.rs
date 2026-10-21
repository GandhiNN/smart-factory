//! A single analog sensor channel - the atomic unit of the simulated process.
//! 
//! One `Signal` is "one thermocouple" or "one moisture probe": it has physical
//! meaning (name + unit), a designed operating band (engineering range), a
//! target the process holds it near (setpoint), a live value, and a quality.

use rand::Rng;

use crate::quality::Quality;

/// One simulated analog sensor channel.
#[derive(Debug, Clone)]
pub struct Signal {
    /// Human/machine-readable identity, e.g. "HeaterZone1/Temperature".
    pub name: String,
    /// Engineering unit, e.g. "degC", "percent", "g".
    pub unit: String,
    /// Low end of the normal operating band (engineering range min).
    pub eu_low: f64,
    /// High end of the normal operating band (engineering range max).
    pub eu_high: f64,
    /// The value the process is trying to hold.
    pub setpoint: f64,
    /// The live value right now.
    pub value: f64,
    /// How strongly `value` is pulled toward `setpoint` each tick (0..1),
    pub responsiveness: f64,
    /// Standard magnitude of random noise added each tick.
    pub noise: f64,
    /// Current trustworthiness of the reading.
    pub quality: Quality,
}

impl Signal {
    /// Create a healthy signal sitting exactly at its setpoint.
    pub fn new(
        name: impl Into<String>,
        unit: impl Into<String>,
        eu_low: f64,
        eu_high: f64,
        setpoint: f64,
    ) -> Self {
        Self {
            name: name.into(),
            unit: unit.into(),
            eu_low,
            eu_high,
            setpoint,
            value: setpoint,
            responsiveness: 0.2,
            noise: 0.05 * (eu_high - eu_low),
            quality: Quality::Good,
        }
    }

    /// Advance the signal one time step.
    /// 
    /// Models first-order lag toward the setpoint plus random noise. If the
    /// sensor has faulted (Bad), the value is left frozen/garbage - a real
    /// dead sensor does not keep tracking the process.
    pub fn tick<R: Rng>(&mut self, rng: &mut R) {
        if self.quality == Quality::Bad {
            // A dead sensor commonly flatlines or reads nonsense. We freeze it
            // and let the quality carry the "don't trust me" signal downstream.
            return;
        }

        // First-order approach to setpoint: new = old + k * (target - old).
        let error = self.setpoint - self.value;
        self.value += self.responsiveness * error;

        // Add symmetric noise in roughly [-noise, +noise].
        let jitter: f64 = rng.gen_range(-1.0..=1.0) * self.noise;
        self.value += jitter;
    }

    /// Is the live value currently outside its designed operating band?
    pub fn out_of_spec(&self) -> bool {
        self.value < self.eu_low || self.value > self.eu_high
    }
}