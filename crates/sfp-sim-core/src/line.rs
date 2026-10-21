//! A whole simulated SFP maker line: a set of sensor channels, a discrete
//! machine state, a throughput counter, and the fault-injection logic that
//! makes the data interesting for downstream analytics.

use rand::Rng;

use crate::quality::Quality;
use crate::signal::Signal;

/// Discrete operating state of the machine.
/// 
/// Unlike the analog `Signal`s, this is categorical. It is the backbone of
/// availability / OEE analytics: "was the line actually producing?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineState {
    /// Producing normally.
    Running,
    /// Intentionally not producing (changeover, idle).
    Stopped,
    /// Tripped by a fault; not producing until reset.
    Faulted,
}

impl MachineState {
    /// Stable small integer encoding - handy when we later expose this over a
    /// protocol that prefers numbers, and for analytics grouping.
    pub fn as_code(self) -> i32 {
        match self {
            MachineState::Stopped => 0,
            MachineState::Running => 1,
            MachineState::Faulted => 2,
        }
    }
}

/// One simulated maker line.
#[derive(Debug, Clone)]
pub struct MakerLine {
    /// Identity of the line, e.g. "Line1".
    pub name: String,
    /// All analog sensor channels on this line, in a fixed order.
    pub signals: Vec<Signal>,
    /// Current discrete machine state.
    pub state: MachineState,
    /// Total sticks produced since start (monotonic while Running).
    pub throughput: u64,
    /// Per-tick probability (0..1) that *some* fault is injected.
    pub fault_rate: f64,
}

impl MakerLine {
    /// Build a representative SFP maker line with a realistic set of channels.
    pub fn new(name: impl Into<String>) -> Self {
        let signals = vec![
            // Two heater zones. Heated-tobacco substrate must stay hot enough
            // to aerosolize but well below combustion - hence a tight band.
            Signal::new("HeaterZone1/Temperature", "degC", 150.0, 175.0, 162.0),
            Signal::new("HeaterZone2/Temperature", "degC", 150.0, 175.0, 165.0),
            // Moisture of the reconstituted tobacco sheet (%). Tight window.
            Signal::new("Sheet/Moisture", "percent", 12.0, 14.0, 13.0),
            // Checkweigher: mass per stick (grams).
            Signal::new("Checkweigher/Weight", "g", 0.85, 0.95, 0.90),
        ];

        Self {
            name: name.into(),
            signals,
            state: MachineState::Running,
            throughput: 0,
            fault_rate: 0.02, // ~2% chance of a fault event per tick
        }
    }

    /// Advance the entire line one cycle.
    pub fn tick<R: Rng>(&mut self, rng: &mut R) {
        self.maybe_inject_fault(rng);

        // Advance every analog channel.
        for signal in &mut self.signals {
            signal.tick(rng);
        }

        // Throughput only grows while actually producing.
        if self.state == MachineState::Running {
            self.throughput += 1;
        }
    }

    /// Randomly degrade the line to create realistic anomalies.
    fn maybe_inject_fault<R: Rng>(&mut self, rng: &mut R) {
        // If currently faulted, give it a chance to recover (operator reset).
        if self.state == MachineState::Faulted {
            if rng.gen_bool(0.1) {
                self.state = MachineState::Running;
                // On recovery, assume sensors come back healthy.
                for s in &mut self.signals {
                    s.quality = Quality::Good;
                }
            }
            return;
        }

        // Otherwise, maybe something goes wrong this tick.
        if !rng.gen_bool(self.fault_rate) {
            return;
        }

        // Pick a fault kind at random.
        match rng.gen_range(0..3) {
            // 0: a random sensor drops out (quality -> Bad).
            0 => {
                if let Some(s) = self.pick_signal(rng) {
                    s.quality = Quality::Bad;
                }
            }
            // 1: a random sensor starts drifting (quality -> Uncertain,
            //      and we shove its value partway out of band).
            1 => {
                if let Some(s) = self.pick_signal(rng) {
                    s.quality = Quality::Uncertain;
                    let span = s.eu_high - s.eu_low;
                    s.value = s.eu_high + 0.1 * span; // nudge out of spec
                }
            }
            // 2: the machine trips and stops producing
            _ => {
                self.state = MachineState::Faulted;
            }
        }
    }

    /// Borrow a random signal mutably, if any exist.
    fn pick_signal<R: Rng>(&mut self, rng: &mut R) -> Option<&mut Signal> {
        if self.signals.is_empty() {
            return None;
        }
        let idx = rng.gen_range(0..self.signals.len());
        self.signals.get_mut(idx)
    }
}