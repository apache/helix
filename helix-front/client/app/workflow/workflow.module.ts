import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { WorkflowListComponent } from './workflow-list/workflow-list.component';
import { WorkflowService } from './shared/workflow.service';
import { SharedModule } from '../shared/shared.module';
import { WorkflowDetailComponent } from './workflow-detail/workflow-detail.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    NgxDatatableModule
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
