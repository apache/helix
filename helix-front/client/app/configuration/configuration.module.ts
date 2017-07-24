import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { ConfigDetailComponent } from './config-detail/config-detail.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  declarations: [
    ConfigDetailComponent
  ],
  exports: [
    ConfigDetailComponent
  ]
})
export class ConfigurationModule { }
