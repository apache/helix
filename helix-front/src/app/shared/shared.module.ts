import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

import { InputDialogComponent } from './dialog/input-dialog/input-dialog.component';
import { DetailHeaderComponent } from './detail-header/detail-header.component';
import { KeyValuePairDirective, KeyValuePairsComponent } from './key-value-pairs/key-value-pairs.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule,
    FlexLayoutModule
  ],
  declarations: [
    InputDialogComponent,
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent
  ],
  entryComponents: [
    InputDialogComponent
  ],
  exports: [
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent
  ]
})
export class SharedModule { }
