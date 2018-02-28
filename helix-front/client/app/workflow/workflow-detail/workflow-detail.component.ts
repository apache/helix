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
  workflowName: string;
  can = false;

  constructor(
    protected route: ActivatedRoute,
    protected service: WorkflowService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    this.clusterName = this.route.snapshot.params['cluster_name'];
    this.workflowName = this.route.snapshot.params['workflow_name'];

    this.service.can().subscribe(data => this.can = data);

    this.loadWorkflow();
  }

  stopWorkflow() {
    this.service.stop(this.clusterName, this.workflowName)
      .subscribe(
        () => {
          this.helper.showSnackBar('Pause command sent.');
          this.loadWorkflow();
        },
        error => this.helper.showError(error)
      );
  }

  resumeWorkflow() {
    this.service.resume(this.clusterName, this.workflowName)
      .subscribe(
        () => {
          this.helper.showSnackBar('Resume command sent.');
          this.loadWorkflow();
        },
        error => this.helper.showError(error)
      );
  }

  protected loadWorkflow() {
    this.isLoading = true;
    this.service.get(this.clusterName, this.workflowName)
      .subscribe(
        workflow => this.workflow = workflow,
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
  }
}
