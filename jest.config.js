/** @type {import('jest').Config} */
module.exports = {
  projects: [
    {
      displayName: 'core',
      preset: 'ts-jest',
      testEnvironment: 'node',
      rootDir: '<rootDir>/packages/core',
      testMatch: ['<rootDir>/src/**/*.spec.ts'],
      moduleNameMapper: {
        '^@ratchet/core$': '<rootDir>/src/index.ts',
      },
    },
    {
      displayName: 'e2e',
      preset: 'ts-jest',
      testEnvironment: 'node',
      rootDir: '<rootDir>',
      testMatch: ['<rootDir>/test/e2e/**/*.e2e.spec.ts'],
      moduleNameMapper: {
        '^@ratchet/core$': '<rootDir>/packages/core/src/index.ts',
        '^@ratchet/store$': '<rootDir>/packages/store/src/index.ts',
        '^@ratchet/worker$': '<rootDir>/apps/worker/src/index.ts',
      },
    },
    {
      displayName: 'chaos',
      preset: 'ts-jest',
      testEnvironment: 'node',
      rootDir: '<rootDir>',
      testMatch: ['<rootDir>/test/chaos/**/*.chaos.spec.ts'],
      moduleNameMapper: {
        '^@ratchet/core$': '<rootDir>/packages/core/src/index.ts',
        '^@ratchet/store$': '<rootDir>/packages/store/src/index.ts',
        '^@ratchet/worker$': '<rootDir>/apps/worker/src/index.ts',
      },
    },
  ],
};