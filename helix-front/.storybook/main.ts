// .storybook/main.ts

// Imports the Storybook's configuration API
import type { StorybookConfig } from '@storybook/core-common';

const config: StorybookConfig = {
  addons: ['@storybook/addon-essentials'],
  core: {
    builder: 'webpack5',
  },
  features: {
    storyStoreV7: true,
  },
  framework: '@storybook/angular',
  stories: ['../src/**/*.stories.@(ts|mdx)'],
  typescript: {
    check: false,
    checkOptions: {},
  },
  webpackFinal: async (config, { configType }) => {
    // Make whatever fine-grained changes you need
    // Return the altered config
    return config;
  },
};
