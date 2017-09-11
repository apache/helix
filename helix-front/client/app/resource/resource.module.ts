import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxJsonViewerModule } from 'ngx-json-viewer';

import { SharedModule } from '../shared/shared.module';
import { ResourceService } from './shared/resource.service';
import { ResourceResolver } from './shared/resource.resolver';
import { ResourceListComponent } from './resource-list/resource-list.component';
import { ResourceDetailComponent } from './resource-detail/resource-detail.component';
import { ResourceDetailForInstanceComponent } from './resource-detail-for-instance/resource-detail-for-instance.component';
import { PartitionListComponent } from './partition-list/partition-list.component';
import { PartitionDetailComponent } from './partition-detail/partition-detail.component';
import { ResourceNodeViewerComponent } from './resource-node-viewer/resource-node-viewer.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    NgxDatatableModule,
    NgxJsonViewerModule,
    SharedModule
  ],
  providers: [
    ResourceService,
    ResourceResolver
  ],
  declarations: [
    ResourceListComponent,
    ResourceDetailComponent,
    ResourceDetailForInstanceComponent,
    PartitionListComponent,
    PartitionDetailComponent,
    ResourceNodeViewerComponent
  ]
})
export class ResourceModule { }
