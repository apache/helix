import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MaterialModule } from '@angular/material';

import { InputDialogComponent } from './dialog/input-dialog/input-dialog.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule
  ],
  declarations: [
  InputDialogComponent
  ],
  entryComponents: [
    InputDialogComponent
  ]
})
export class SharedModule { }
