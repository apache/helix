import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';
import { FormsModule } from '@angular/forms';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxJsonViewerModule } from 'ngx-json-viewer';

import { InputDialogComponent } from './dialog/input-dialog/input-dialog.component';
import { DetailHeaderComponent } from './detail-header/detail-header.component';
import { KeyValuePairDirective, KeyValuePairsComponent } from './key-value-pairs/key-value-pairs.component';
import { JsonViewerComponent } from './json-viewer/json-viewer.component';
import { AlertDialogComponent } from './dialog/alert-dialog/alert-dialog.component';
import { StateLabelComponent } from './state-label/state-label.component';
import { NodeViewerComponent } from './node-viewer/node-viewer.component';
import { InputInlineComponent } from './input-inline/input-inline.component';
import { DataTableComponent } from './data-table/data-table.component';
import { ConfirmDialogComponent } from './dialog/confirm-dialog/confirm-dialog.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    FlexLayoutModule,
    FormsModule,
    NgxDatatableModule,
    NgxJsonViewerModule
  ],
  declarations: [
    InputDialogComponent,
    AlertDialogComponent,
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent,
    JsonViewerComponent,
    StateLabelComponent,
    NodeViewerComponent,
    InputInlineComponent,
    DataTableComponent,
    ConfirmDialogComponent
  ],
  entryComponents: [
    InputDialogComponent,
    AlertDialogComponent,
    ConfirmDialogComponent
  ],
  exports: [
    RouterModule,
    MaterialModule,
    FlexLayoutModule,
    FormsModule,
    NgxJsonViewerModule,
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent,
    JsonViewerComponent,
    StateLabelComponent,
    NodeViewerComponent
  ]
})
export class SharedModule { }
