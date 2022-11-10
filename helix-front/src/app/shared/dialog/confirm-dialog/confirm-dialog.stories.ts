import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import {
  moduleMetadata,
  Meta,
  Story,
  componentWrapperDecorator,
} from '@storybook/angular';

import { ConfirmDialogTestComponent } from './confirm-dialog-test.component';
import { SharedModule } from '../../shared.module';
import { MaterialModule } from '../../material.module';
import { FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { ConfirmDialogComponent } from './confirm-dialog.component';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';

export default {
  component: ConfirmDialogTestComponent,
  title: 'Confirm Dialog Test',
  excludeStories: /.*Data$/,
  decorators: [
    moduleMetadata({
      imports: [SharedModule, MaterialModule, ReactiveFormsModule],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }),
    //👇 Wraps our stories with a decorator
    componentWrapperDecorator(
      (story) => `<div style="margin: 3em">${story}</div>`
    ),
  ],
} as Meta;

const Template: Story = (args) => ({
  component: ConfirmDialogTestComponent,
  template: `<hi-confirm-dialog-test [data]="data"></hi-confirm-dialog-test>`,
  props: {
    ...args,
  },
});

export const Delete = Template.bind({});
Delete.args = {
  data: {
    title: 'Confirm Delete',
    message:
      'Are you sure you want to delete this cluster? This cannot be undone.',
    confirmButtonText: "Yes, I'm sure",
  },
};
