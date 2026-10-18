import { calculateBackoffWithJitter } from '../retry';

describe('calculateBackoffWithJitter', () => {
  it('scales exponentially and bounds within [cap/2, cap]', () => {
    const policy = { baseDelayMs: 100, maxDelayMs: 1000 };

    // attempt 1 -> cap = 100 -> [50, 100]
    expect(calculateBackoffWithJitter(1, policy, () => 0)).toBe(50);
    expect(calculateBackoffWithJitter(1, policy, () => 0.999)).toBe(99);

    // attempt 3 -> cap = 400 -> [200, 400]
    expect(calculateBackoffWithJitter(3, policy, () => 0)).toBe(200);
    expect(calculateBackoffWithJitter(3, policy, () => 0.5)).toBe(300);

    // attempt 10 -> capped at maxDelayMs (1000) -> [500, 1000]
    expect(calculateBackoffWithJitter(10, policy, () => 0)).toBe(500);
    expect(calculateBackoffWithJitter(10, policy, () => 0.999)).toBe(999);
  });
});