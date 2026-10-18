export interface RetryPolicy {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 5,
  baseDelayMs: 500,
  maxDelayMs: 30_000,
};

/**
 * Computes exponential backoff with equal jitter:
 * cap = min(maxDelayMs, baseDelayMs * 2^(attempt - 1))
 * delay = cap/2 + random() * (cap/2)
 */
export function calculateBackoffWithJitter(
  attempt: number,
  policy: Partial<RetryPolicy> = {},
  randomFn: () => number = Math.random,
): number {
  const baseDelayMs = policy.baseDelayMs ?? DEFAULT_RETRY_POLICY.baseDelayMs;
  const maxDelayMs = policy.maxDelayMs ?? DEFAULT_RETRY_POLICY.maxDelayMs;
  const normalizedAttempt = Math.max(1, attempt);

  const exponentialCap = Math.min(
    maxDelayMs,
    baseDelayMs * Math.pow(2, normalizedAttempt - 1),
  );

  const half = exponentialCap / 2;
  return Math.floor(half + randomFn() * half);
}