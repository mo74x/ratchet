export type DurationInput = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;

const DURATION_REGEX = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/;

const UNIT_TO_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

export function parseDurationMs(input: DurationInput): number {
  if (typeof input === 'number') {
    if (!Number.isFinite(input) || input < 0) {
      throw new Error(`Invalid duration number: ${input}`);
    }
    return Math.floor(input);
  }

  const match = DURATION_REGEX.exec(input.trim());
  if (!match) {
    throw new Error(
      `Invalid duration string "${input}". Expected format like "500ms", "10s", "5m", "2h", or "3d".`,
    );
  }

  const value = parseFloat(match[1]);
  const unit = match[2];
  return Math.floor(value * UNIT_TO_MS[unit]);
}