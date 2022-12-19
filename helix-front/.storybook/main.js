// .storybook/main.js
const path = require('path');

module.exports = {
  addons: ['@storybook/addon-essentials'],
  core: {
    builder: 'webpack5',
  },
  features: {
    storyStoreV7: true,
  },
  framework: '@storybook/angular',
  stories: ['../src/app/**/*.stories.@(ts|mdx)'],
  typescript: {
    check: false,
    checkOptions: {},
  },
  webpackFinal: async (config, { configType }) => {
    // Make whatever fine-grained changes you need
    // Return the altered config
    return {
      ...config,
      resolve: {
        ...config.resolve,
        modules: [path.resolve('./src'), ...config.resolve.modules],
        fallback: {
          timers: false,
          tty: false,
          os: false,
          http: false,
          https: false,
          zlib: false,
          util: false,
          ...config.resolve.fallback,
        },
      },
    };
  },
};
