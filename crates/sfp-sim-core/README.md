# What is a sensor reading?

It's not just a number. It's:
- a **value** (e.g. 162.3),
- a **unit** and physical meaning (°C, a heater zone),
- an **engineering range** (this zone operates 150-175 °C),
- and a **quality** (is the sensor actually working, or is this garbage?).

OPC-UA formalizes exactly this (value + status + timestamp + metadata). So the core sim will produce values with quality, because quality is a first-class physical reality - thermocouples do fail, and the whole downstream analytics story depends on distinguishing "0 °C" from "sensor dead."

# What is the process?

We will model one SFP _maker line_ with a few signals:
- 2 heater zones (temperature, °C) - must stay in a safe band (heated tobacco must not combust).
- 1 moisture sensor (%) on the reconstituted tobacco sheet.
- 1 checkweigher (grams per stick).
- a machine **state** (Running / Stopped / Faulted) and a **throughput** counter (sticks made).

# How do values evolve?

Each tick (say every 200 ms), each signal moves toward its setpoint with a bit of noise. We also inject faults probabilistically: a sensor can drop out (quality goes Bad), drift, or the machine can fault and stop.

# What does one sensor channel need to know about itself?

1. **Identity & meaning**: a name (`HeaterZone1/Temperature`), a unit (`°C`). Metadata, not just a number.
2. **Engineering range**: the min/max it's designed to operate in (e.g. 150-175 °C). This is what makes a value _interpretable_ - "162" means nothing without "normal band is 150-175."
3. **A setpoint**: the value the process is trying to hold.
4. **Current value**: where it actually is right now.
5. **Dynamics**: each tick it moves toward the setpoint, plus noise. Real processes don't teleport to setpoint; they ramp with wobble.
6. **Quality**: healthy by default, but can degrade (drift -> Uncertain, dropout -> Bad).

The core behavior is one method, `tick()`, that advances the signal one time step. This is a tiny physics model: _value drifts toward setpoint, with Gaussian-ish noise, unless the sensor has faulted_.

## What is a `setpoint`?

A **setpoint** is the target value the process is trying to hold a signal at i.e. the "desired" value, as opposed to the actual measured value.

The term comes from control theory. In any control loop we have three things:

- **Setpoint (SP)** - what we _want_ the value to be -> "Keep this heater zone at 162 °C."
- **Process variable (PV)** - what the value _actually_ is right now, as measured by the sensor. "It's currently reading 159.4 °C."
- **Control output** - what the controller does to close the gap ("turn the heater element up/down").

The controller (a "PID loop" in real hardware, living in the PLC) constantly compares PV to SP and drives the actuator to minimize the **error** which is SP - PV.

### Concrete SFP example

An operator or recipe says the drying zone must run at **162 °C** - that's the setpoint. 
The thermocouple reads the real temperature (the "process variable"), which wobbles around 162 due to heat loss, product moving through, element cycling, etc.
The control system keeps nudging the heater to pull the actual value back toward 162.

# What is a "line" in a plant?

It's:

1. **A collection of sensor channels** that belong to one machine and move together each cycle.
2. **A machine state** - Running / Stopped / Faulted. This is discrete, not analog. It's the single most important signal for OEE/availability anlytics ("was the machine up?").
3. **Throughput** - a monotonically increasing counter (sticks produced). Only increases while Running.
4. **Fault behavior** - real machines degrade: sensors drop out, values drift, the machine trips and stops. We inject these probabilistically so downstream analytics has something to detech.

## OPC-UA Overflow

An OPC-UA `StatusCode` is a 32-but value: the top bits are the severity/code (Good/Uncertain/Bad + reason), and lower "info bits" carry flags. The **Overflow** bit means _"this subscription's queue overflowed - at least one notification was dropped before this one."_

Why it's happening: your monitored items use the default queue size of 1. When the server produces values faster than the client drains them for a moment, the single-slot queue overflows and the server sets the Overflow bit on the next delivered value. At a 500ms interval with 6 items, it's harmess and intermittent.

