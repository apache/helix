module.exports = async function (globalConfig, projectConfig) {
  'use strict';
  var testing_1 = require('@angular/core/testing');
  var testing_2 = require('@angular/platform-browser-dynamic/testing');
  require('jest-preset-angular');
  require('zone.js');
  require('zone.js/testing');
  // First, initialize the Angular testing environment.
  testing_1
    .getTestBed()
    .initTestEnvironment(
      testing_2.BrowserDynamicTestingModule,
      testing_2.platformBrowserDynamicTesting(),
      {
        teardown: { destroyAfterEach: true },
      }
    );
  Object.defineProperty(window, 'CSS', { value: null });
  Object.defineProperty(window, 'getComputedStyle', {
    value: function () {
      return {
        display: 'none',
        appearance: ['-webkit-appearance'],
      };
    },
  });
  Object.defineProperty(document, 'doctype', {
    value: '<!DOCTYPE html>',
  });
  Object.defineProperty(document.body.style, 'transform', {
    value: function () {
      return {
        enumerable: true,
        configurable: true,
      };
    },
  });
};
