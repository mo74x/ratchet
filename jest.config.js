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
  ],
};