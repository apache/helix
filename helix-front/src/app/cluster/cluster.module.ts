import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpModule } from '@angular/http';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import 'hammerjs';
import { FlexLayoutModule } from '@angular/flex-layout';

import { ClusterService } from './shared/cluster.service';
import { ClusterResolver } from './shared/cluster.resolver';
import { ClusterListComponent } from './cluster-list/cluster-list.component';
import { ClusterDetailComponent } from './cluster-detail/cluster-detail.component';

@NgModule({
  imports: [
    CommonModule,
    HttpModule,
    RouterModule,
    MaterialModule,
    BrowserAnimationsModule,
    FlexLayoutModule
  ],
  declarations: [
    ClusterListComponent,
    ClusterDetailComponent
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
