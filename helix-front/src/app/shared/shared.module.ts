import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { InputDialogComponent } from './dialog/input-dialog/input-dialog.component';
import { DetailHeaderComponent } from './detail-header/detail-header.component';
import { KeyValuePairDirective, KeyValuePairsComponent } from './key-value-pairs/key-value-pairs.component';
import { JsonViewerComponent } from './json-viewer/json-viewer.component';
import { AlertDialogComponent } from './dialog/alert-dialog/alert-dialog.component';
import { StateLabelComponent } from './state-label/state-label.component';
import { NodeViewerComponent } from './node-viewer/node-viewer.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    FlexLayoutModule,
    NgxDatatableModule
  ],
  declarations: [
    InputDialogComponent,
    AlertDialogComponent,
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent,
    JsonViewerComponent,
    StateLabelComponent,
    NodeViewerComponent
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
    StateLabelComponent,
    NodeViewerComponent
  ]
})
export class SharedModule { }
