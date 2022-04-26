import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FlexLayoutModule } from '@angular/flex-layout';
import { FormsModule } from '@angular/forms';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxJsonViewerModule } from 'ngx-json-viewer';

import { HelperService } from './helper.service';
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
import { DisabledLabelComponent } from './disabled-label/disabled-label.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
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
    ConfirmDialogComponent,
    DisabledLabelComponent
  ],
  // In your application projects, you can remove entryComponents
  // NgModules and any uses of ANALYZE_FOR_ENTRY_COMPONENTS.
  // They are no longer required with the Ivy compiler and runtime.
  // You may need to keep these if building a library that will
  // be consumed by a View Engine application.
  //  https://update.angular.io/?l=3&v=9.1-10.2
  //
  // entryComponents: [
  //   InputDialogComponent,
  //   AlertDialogComponent,
  //   ConfirmDialogComponent
  // ],
  exports: [
    RouterModule,
    FlexLayoutModule,
    FormsModule,
    NgxJsonViewerModule,
    DetailHeaderComponent,
    KeyValuePairDirective,
    KeyValuePairsComponent,
    JsonViewerComponent,
    StateLabelComponent,
    NodeViewerComponent,
    DisabledLabelComponent
  ],
  providers: [
    HelperService
  ]
})
export class SharedModule { }
