import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MaterialModule } from '@angular/material';

import { InputDialogComponent } from './dialog/input-dialog/input-dialog.component';
import { DetailHeaderComponent } from './detail-header/detail-header.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule
  ],
  declarations: [
    InputDialogComponent,
    DetailHeaderComponent
  ],
  entryComponents: [
    InputDialogComponent
  ],
  exports: [
    DetailHeaderComponent
  ]
})
export class SharedModule { }
