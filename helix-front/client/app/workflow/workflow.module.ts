import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { NgxGraphModule } from '@swimlane/ngx-graph';

import { WorkflowListComponent } from './workflow-list/workflow-list.component';
import { WorkflowService } from './shared/workflow.service';
import { SharedModule } from '../shared/shared.module';
import { WorkflowDetailComponent } from './workflow-detail/workflow-detail.component';
import { WorkflowDagComponent } from './workflow-dag/workflow-dag.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    NgxDatatableModule,
    NgxChartsModule,
    NgxGraphModule
  ],
  providers: [
    WorkflowService
  ],
  declarations: [
    WorkflowListComponent,
    WorkflowDetailComponent,
    WorkflowDagComponent
  ]
})
export class WorkflowModule { }
