import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';

import { SharedModule } from '../shared/shared.module';
import { ClusterService } from './shared/cluster.service';
import { ClusterResolver } from './shared/cluster.resolver';
import { ClusterListComponent } from './cluster-list/cluster-list.component';
import { ClusterDetailComponent } from './cluster-detail/cluster-detail.component';
import { ClusterComponent } from './cluster.component';

@NgModule({
  imports: [CommonModule, HttpClientModule, SharedModule],
  declarations: [
    ClusterListComponent,
    ClusterDetailComponent,
    ClusterComponent,
  ],
  providers: [ClusterService, ClusterResolver],
  exports: [ClusterListComponent, ClusterDetailComponent],
})
export class ClusterModule {}
