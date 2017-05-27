import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { SharedModule } from '../shared/shared.module';
import { ResourceService } from './shared/resource.service';
import { ResourceResolver } from './shared/resource.resolver';
import { ResourceListComponent } from './resource-list/resource-list.component';
import { ResourceDetailComponent } from './resource-detail/resource-detail.component';
import { ResourceDetailForInstanceComponent } from './resource-detail-for-instance/resource-detail-for-instance.component';
import { PartitionListComponent } from './partition-list/partition-list.component';
import { PartitionDetailComponent } from './partition-detail/partition-detail.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    NgxDatatableModule,
    SharedModule
  ],
  providers: [
    ResourceService,
    ResourceResolver
  ],
  declarations: [ResourceListComponent, ResourceDetailComponent, ResourceDetailForInstanceComponent, PartitionListComponent, PartitionDetailComponent]
})
export class ResourceModule { }
