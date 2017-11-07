import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { WorkflowService } from '../shared/workflow.service';

@Component({
  selector: 'hi-workflow-list',
  templateUrl: './workflow-list.component.html',
  styleUrls: ['./workflow-list.component.scss']
})
export class WorkflowListComponent implements OnInit {

  isLoading = true;
  clusterName: string;
  workflows: string[];

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private service: WorkflowService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.isLoading = true;
      this.clusterName = this.route.parent.snapshot.params['name'];

      this.service
        .getAll(this.clusterName)
        .subscribe(
          workflows => this.workflows = workflows,
          error => console.log(error),
          () => this.isLoading = false
        );
    }
  }

}
