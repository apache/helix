import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { SharedModule } from '../shared/shared.module';
import { InstanceListComponent } from './instance-list/instance-list.component';
import { InstanceDetailComponent } from './instance-detail/instance-detail.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    NgxDatatableModule,
    SharedModule
  ],
  declarations: [InstanceListComponent, InstanceDetailComponent]
})
export class InstanceModule { }
