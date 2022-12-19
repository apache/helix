import { APP_BASE_HREF } from '@angular/common';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import {
  moduleMetadata,
  Meta,
  Story,
  componentWrapperDecorator,
} from '@storybook/angular';

import { StateLabelComponent } from './state-label.component';
import { SharedModule } from '../shared.module';
import { MaterialModule } from '../material.module';
import { isMainThread } from 'worker_threads';

export default {
  component: StateLabelComponent,
  title: 'State Label',
  excludeStories: /.*Data$/,
  decorators: [
    moduleMetadata({
      imports: [SharedModule, MaterialModule],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }),
    //👇 Wraps our stories with a decorator
    componentWrapperDecorator(
      (story) => `<div style="margin: 3em">${story}</div>`
    ),
  ],
} as Meta;

const Template: Story = (args: any) => ({
  props: {
    ...args,
  },
  template: `
  <hi-state-label [state]="state" [isReady]="isReady"></hi-state-label>
  `,
});

// States for Helix are defined here:
// https://helix.apache.org/Concepts.html
export const Offline = Template.bind({});
Offline.args = {
  state: 'OFFLINE',
  isReady: false,
};

export const Standby = Template.bind({});
Standby.args = {
  state: 'STANDBY',
  isReady: true,
};

export const Leader = Template.bind({});
Leader.args = {
  state: 'LEADER',
  isReady: true,
};
