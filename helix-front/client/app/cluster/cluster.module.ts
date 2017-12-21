import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpModule } from '@angular/http';

import { SharedModule } from '../shared/shared.module';
import { ClusterService } from './shared/cluster.service';
import { ClusterResolver } from './shared/cluster.resolver';
import { ClusterListComponent } from './cluster-list/cluster-list.component';
import { ClusterDetailComponent } from './cluster-detail/cluster-detail.component';
import { ClusterComponent } from './cluster.component';

@NgModule({
  imports: [
    CommonModule,
    HttpModule,
    SharedModule
  ],
  declarations: [
    ClusterListComponent,
    ClusterDetailComponent,
    ClusterComponent
  ],
  providers: [
    ClusterService,
    ClusterResolver
  ],
  exports: [
    ClusterListComponent,
    ClusterDetailComponent
  ]
})
export class ClusterModule { }
