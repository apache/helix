import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Workflow } from '../shared/workflow.model';
import { WorkflowService } from '../shared/workflow.service';
import { HelperService } from '../../shared/helper.service';

@Component({
  selector: 'hi-workflow-detail',
  templateUrl: './workflow-detail.component.html',
  styleUrls: ['./workflow-detail.component.scss']
})
export class WorkflowDetailComponent implements OnInit {

  isLoading = true;
  workflow: Workflow;
  clusterName: string;

  constructor(
    protected route: ActivatedRoute,
    protected service: WorkflowService,
    protected helper: HelperService
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
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
  }
}
