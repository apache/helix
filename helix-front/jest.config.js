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
    'vis-data': '<rootDir>/node_modules/vis-data/dist/umd.js',
    'vis-network': '<rootDir>/node_modules/vis-network/dist/vis-network.js',
    'vis-timeline':
      '<rootDir>/node_modules/vis-timeline/dist/vis-timeline-graph2d.min.js',
  },
  setupFilesAfterEnv: ['<rootDir>/setupJest.ts'],
  globalSetup: 'jest-preset-angular/global-setup',
};
