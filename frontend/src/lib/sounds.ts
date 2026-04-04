/**
 * Sound alerts using Web Audio API — no external files needed.
 * All sounds are generated programmatically via OscillatorNode.
 */

let _ctx: AudioContext | null = null

function ctx(): AudioContext {
  if (!_ctx) _ctx = new AudioContext()
  // Resume if suspended (browser autoplay policy)
  if (_ctx.state === 'suspended') _ctx.resume()
  return _ctx
}

function tone(
  freq: number,
  durationSec: number,
  type: OscillatorType = 'sine',
  peakGain = 0.25,
  startDelaySec = 0,
) {
  const c = ctx()
  const osc = c.createOscillator()
  const gain = c.createGain()
  osc.connect(gain)
  gain.connect(c.destination)
  osc.type = type
  osc.frequency.value = freq
  const t0 = c.currentTime + startDelaySec
  gain.gain.setValueAtTime(0, t0)
  gain.gain.linearRampToValueAtTime(peakGain, t0 + 0.01)
  gain.gain.exponentialRampToValueAtTime(0.001, t0 + durationSec)
  osc.start(t0)
  osc.stop(t0 + durationSec + 0.01)
}

/** Double-beep ascending — order filled */
export function playOrderFill() {
  tone(880, 0.12, 'sine', 0.25, 0)
  tone(1100, 0.12, 'sine', 0.2, 0.13)
}

/** Low buzz — order rejected */
export function playOrderReject() {
  tone(220, 0.3, 'sawtooth', 0.2, 0)
  tone(180, 0.25, 'sawtooth', 0.15, 0.05)
}

/** Triple short beep — generic alert */
export function playAlert() {
  tone(660, 0.08, 'sine', 0.2, 0)
  tone(660, 0.08, 'sine', 0.2, 0.12)
  tone(880, 0.12, 'sine', 0.25, 0.24)
}

/** Single tick — new order placed */
export function playOrderPlaced() {
  tone(740, 0.1, 'triangle', 0.2, 0)
}

/** Soft chime — connection restored */
export function playConnected() {
  tone(523, 0.15, 'sine', 0.15, 0)
  tone(659, 0.2, 'sine', 0.15, 0.1)
}
