module.exports = {
  preset: 'jest-preset-angular',
  roots: ['./src'],
  testMatch: ['**/+(*.)+(spec).+(ts)'],
  collectCoverage: true,
  coverageReporters: ['html', 'lcov', 'json', 'text', 'jest-html-reporters'],
  coverageDirectory: './coverage',
  moduleFileExtensions: ['ts', 'html', 'js', 'json'],
  moduleNameMapper: {
    'ace-builds': '<rootDir>/node_modules/ace-builds/src-noconflict/ace.js',
  },
};
