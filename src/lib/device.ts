export const DEVICE_ID_KEY = "pf.deviceId.v1";

function uuidv4() {
  // Prefer crypto.randomUUID when available.
  if (typeof globalThis.crypto?.randomUUID === "function") return globalThis.crypto.randomUUID();
  // Fallback when crypto.randomUUID is unavailable (non-cryptographic; local device id only).
  const s4 = () => Math.floor((1 + Math.random()) * 0x10000).toString(16).slice(1);
  return `${s4()}${s4()}-${s4()}-${s4()}-${s4()}-${s4()}${s4()}${s4()}`;
}

export function getOrCreateDeviceId() {
  if (typeof window === "undefined") return "server";
  const existing = localStorage.getItem(DEVICE_ID_KEY);
  if (existing) return existing;
  const id = uuidv4();
  localStorage.setItem(DEVICE_ID_KEY, id);
  return id;
}

