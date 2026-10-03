/** Decode standard 0x2A37 notifications; BLE RR ticks are 1/1024 seconds. */
export function decodeHeartRate(bytes: DataView) {
  if (bytes.byteLength < 2) throw new Error('Truncated heart rate notification');
  const flags = bytes.getUint8(0);
  let offset = 1;
  const read16 = () => {
    if (offset + 2 > bytes.byteLength) throw new Error('Truncated BLE field');
    const value = bytes.getUint16(offset, true); offset += 2; return value;
  };
  const heart_rate = flags & 1 ? read16() : bytes.getUint8(offset++);
  if (flags & 8) read16(); // Optional Energy Expended precedes RR fields.
  const rr_intervals_ms: number[] = [];
  if (flags & 16) while (offset < bytes.byteLength) rr_intervals_ms.push(read16() * 1000 / 1024);
  return {heart_rate, rr_intervals_ms, sensor_contact: flags & 4 ? Boolean(flags & 2) : null};
}
