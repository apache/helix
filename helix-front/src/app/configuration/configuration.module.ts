import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { ConfigDetailComponent } from './config-detail/config-detail.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule,
    FlexLayoutModule,
    NgxDatatableModule
  ],
  declarations: [
    ConfigDetailComponent
  ],
  exports: [
    ConfigDetailComponent
  ]
})
export class ConfigurationModule { }
