// .storybook/main.js

module.exports = {
  addons: ['@storybook/addon-essentials'],
  babel: async (options) => ({
    // Update your babel configuration here
    ...options,
  }),
  core: {
    builder: 'webpack5',
  },
  features: {
    storyStoreV7: true,
  },
  framework: '@storybook/angular',
  stories: ['../src/**/*.stories.@(js|mdx)'],
  webpackFinal: async (config, { _configType }) => {
    // Make whatever fine-grained changes you need
    // Return the altered config
    return config;
  },
};
