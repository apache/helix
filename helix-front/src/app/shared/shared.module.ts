import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

import { InputDialogComponent } from './dialog/input-dialog/input-dialog.component';
import { DetailHeaderComponent } from './detail-header/detail-header.component';
import { KeyValuePairDirective, KeyValuePairsComponent } from './key-value-pairs/key-value-pairs.component';
import { JsonViewerComponent } from './json-viewer/json-viewer.component';
import { AlertDialogComponent } from './dialog/alert-dialog/alert-dialog.component';
import { StateLabelComponent } from './state-label/state-label.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    FlexLayoutModule
  ],
  declarations: [
    InputDialogComponent,
    AlertDialogComponent,
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent,
    JsonViewerComponent,
    StateLabelComponent
  ],
  entryComponents: [
    InputDialogComponent,
    AlertDialogComponent
  ],
  exports: [
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent,
    JsonViewerComponent,
    StateLabelComponent
  ]
})
export class SharedModule { }
