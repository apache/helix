import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { NgxGraphModule } from 'ngx-dag';

import { WorkflowListComponent } from './workflow-list/workflow-list.component';
import { WorkflowService } from './shared/workflow.service';
import { JobService } from './shared/job.service';
import { SharedModule } from '../shared/shared.module';
import { WorkflowDetailComponent } from './workflow-detail/workflow-detail.component';
import { WorkflowDagComponent } from './workflow-dag/workflow-dag.component';
import { JobListComponent } from './job-list/job-list.component';
import { JobDetailComponent } from './job-detail/job-detail.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    NgxDatatableModule,
    NgxChartsModule,
    NgxGraphModule
  ],
  providers: [
    WorkflowService,
    JobService
  ],
  declarations: [
    WorkflowListComponent,
    WorkflowDetailComponent,
    WorkflowDagComponent,
    JobListComponent,
    JobDetailComponent
  ]
})
export class WorkflowModule { }
