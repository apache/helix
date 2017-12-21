import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ClipboardModule } from 'ngx-clipboard';

import { SharedModule } from '../shared/shared.module';
import { HistoryListComponent } from './history-list/history-list.component';

@NgModule({
  imports: [
    CommonModule,
    NgxDatatableModule,
    ClipboardModule,
    SharedModule
  ],
  declarations: [HistoryListComponent]
})
export class HistoryModule { }
