module.exports = {
  preset: 'jest-preset-angular',
  roots: ['./src'],
  testMatch: ['**/+(*.)+(spec).+(ts)'],
  collectCoverage: true,
  coverageReporters: ['html', 'lcov', 'json', 'text', 'jest-html-reporters'],
  coverageDirectory: './coverage',
  moduleFileExtensions: ['ts', 'html', 'js', 'json'],
};
