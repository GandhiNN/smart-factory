//! Signal quality - the OPC-UA independent notion of "is this reading trustworthy?"
//! 
//! Real sensors don't just produce numbers; they produce numbers that may be
//! good, suspect, or outright invalid. Modeling this here (rather than only at
//! the protocol layer) keeps quality a first-class property of the process,
//! exactly as it is in a real plant.

/// The trustworthiness of a single sensor reading.
/// 
/// This mirrors the three-way split OPC-UA uses (Good / Uncertain / Bad),
/// but is deliberately defined in our own domain terms so this crate has no
/// dependency on any protocol library.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    /// Sensor healthy, value is reliable.
    Good,
    /// Value is usable but suspect (e.g. sensor drifting, out of calibration).
    Uncertain,
    /// Sensor has failed; the accompanying value is meaningless
    Bad,
}

impl Quality {
    /// Convenience: is this reading safe to use in control/analytics as-is?
    pub fn is_usable(self) -> bool {
        matches!(self, Quality::Good | Quality::Uncertain)
    }
}