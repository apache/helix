import { Component, OnInit, Input, ViewChild } from '@angular/core';

import moment from 'moment';

import { Settings } from '../../core/settings';
import { Job } from '../shared/workflow.model';
import { DatatableComponent } from '@swimlane/ngx-datatable';

@Component({
  selector: 'hi-job-list',
  templateUrl: './job-list.component.html',
  styleUrls: ['./job-list.component.scss'],
})
export class JobListComponent implements OnInit {
  @Input()
  jobs: Job[];

  @ViewChild('jobsTable', { static: false })
  table: DatatableComponent;

  rowHeight = Settings.tableRowHeight;
  headerHeight = Settings.tableHeaderHeight;
  sorts = [
    { prop: 'startTime', dir: 'desc' },
    { prop: 'name', dir: 'asc' },
  ];
  messages = {
    emptyMessage: 'The list is empty.',
    totalMessage: 'total',
    selectedMessage: 'selected',
  };

  constructor() {}

  ngOnInit() {}

  parseTime(rawTime: string): string {
    return moment(parseInt(rawTime)).fromNow();
  }

  onSelect({ selected }) {
    const row = selected[0];

    this.table.rowDetail.toggleExpandRow(row);
  }
}
