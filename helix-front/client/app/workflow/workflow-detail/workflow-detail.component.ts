import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as moment from 'moment';

import { Settings } from '../../core/settings';
import { Workflow } from '../shared/workflow.model';
import { WorkflowService } from '../shared/workflow.service';

@Component({
  selector: 'hi-workflow-detail',
  templateUrl: './workflow-detail.component.html',
  styleUrls: ['./workflow-detail.component.scss']
})
export class WorkflowDetailComponent implements OnInit {

  isLoading = true;
  workflow: Workflow;
  clusterName: string;

  rowHeight = Settings.tableRowHeight;
  headerHeight = Settings.tableHeaderHeight;
  sorts = [
    { prop: 'startTime', dir: 'desc'},
    { prop: 'name', dir: 'asc'}
  ];
  messages = {
    emptyMessage: 'The queue is empty.',
    totalMessage: 'total',
    selectedMessage: 'selected'
  };

  constructor(
    private route: ActivatedRoute,
    private service: WorkflowService
  ) { }

  ngOnInit() {
    this.clusterName = this.route.snapshot.params['cluster_name'];

    this.service
      .get(
        this.route.snapshot.params['cluster_name'],
        this.route.snapshot.params['workflow_name']
      )
      .subscribe(
        workflow => this.workflow = workflow,
        error => console.log(error),
        () => this.isLoading = false
      );
  }

  parseTime(rawTime: string): string {
    return moment(parseInt(rawTime)).fromNow();
  }

  onSelect({ selected }) {
    const row = selected[0];
    // this.table.rowDetail.toggleExpandRow(row);
  }

}
