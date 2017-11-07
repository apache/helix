import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ClipboardModule } from 'ngx-clipboard';

import { HistoryListComponent } from './history-list/history-list.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    NgxDatatableModule,
    ClipboardModule
  ],
  declarations: [HistoryListComponent]
})
export class HistoryModule { }
