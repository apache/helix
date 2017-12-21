import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { VisModule } from 'ngx-vis';
import { NgxChartsModule } from '@swimlane/ngx-charts';

import { SharedModule } from '../shared/shared.module';
import { DashboardComponent } from './dashboard.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    VisModule,
    NgxChartsModule
  ],
  declarations: [
    DashboardComponent
  ]
})
export class DashboardModule { }
