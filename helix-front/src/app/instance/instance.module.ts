import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

import { SharedModule } from '../shared/shared.module';
import { InstanceListComponent } from './instance-list/instance-list.component';
import { InstanceDetailComponent } from './instance-detail/instance-detail.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    FlexLayoutModule,
    SharedModule
  ],
  declarations: [InstanceListComponent, InstanceDetailComponent]
})
export class InstanceModule { }
