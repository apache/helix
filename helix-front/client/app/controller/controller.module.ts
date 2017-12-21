import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { ControllerDetailComponent } from './controller-detail/controller-detail.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  declarations: [ControllerDetailComponent]
})
export class ControllerModule { }
