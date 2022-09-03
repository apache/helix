import { APP_BASE_HREF } from '@angular/common';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import {
  moduleMetadata,
  Meta,
  Story,
  componentWrapperDecorator,
} from '@storybook/angular';

import { WorkflowListComponent } from './workflow-list.component';
import { WorkflowModule } from '../workflow.module';
import { MaterialModule } from 'app/shared/material.module';
import { SharedModule } from 'app/shared/shared.module';

const routes: Routes = [
  { path: 'workflows', component: WorkflowListComponent },
];

export default {
  component: WorkflowListComponent,
  title: 'Workflow List',
  excludeStories: /.*Data$/,
  decorators: [
    moduleMetadata({
      imports: [
        WorkflowModule,
        SharedModule,
        MaterialModule,
        RouterModule.forRoot(routes, { useHash: true }),
      ],
      providers: [
        {
          provide: APP_BASE_HREF,
          useValue: '/',
        },
      ],
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
  <div>Workflow List</div>
  <span>There's no workflow here.</span>
  <hi-workflow-list [workflowRows]="workflowRows" [clusterName]="clusterName" [isLoading]="isLoading"></hi-workflow-list>
   <router-outlet></router-outlet>
`,
});

const Default = Template.bind({});

const workflowRows = [{ name: 'workflow1' }];
const clusterName = 'cluster1';

Default.args = {
  workflowRows,
  clusterName,
  headerHeight: 40,
  rowHeight: 40,
  isLoading: false,
};

const Loading = Template.bind({});
Loading.args = {
  workflowRows: [],
  isLoading: true,
};

const Empty = Template.bind({});
Empty.args = {
  // Shaping the stories through args composition.
  // Inherited data coming from the Loading story.
  ...Loading.args,
  isLoading: false,
};
