import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { WorkflowListComponent } from './workflow-list/workflow-list.component';
import { WorkflowService } from './shared/workflow.service';
import { SharedModule } from '../shared/shared.module';
import { WorkflowDetailComponent } from './workflow-detail/workflow-detail.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  providers: [
    WorkflowService
  ],
  declarations: [
    WorkflowListComponent,
    WorkflowDetailComponent
  ]
})
export class WorkflowModule { }
