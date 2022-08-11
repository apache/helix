import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Settings } from '../../core/settings';
import { WorkflowService } from '../shared/workflow.service';

type WorkflowRow = {
  name: string;
};

@Component({
  selector: 'hi-workflow-list',
  templateUrl: './workflow-list.component.html',
  styleUrls: ['./workflow-list.component.scss'],
})
export class WorkflowListComponent implements OnInit {
  @ViewChild('workflowsTable', { static: true })
  table: any;

  isLoading = true;
  clusterName: string;
  workflows: string[];
  workflowRows: WorkflowRow[];

  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;

  sorts = [{ prop: 'name', dir: 'asc' }];

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private service: WorkflowService
  ) {}

  ngOnInit() {
    if (this.route.parent) {
      this.isLoading = true;
      this.clusterName = this.route.parent.snapshot.params['name'];
      console.log(
        'this.route.parent from workflow-detail.component',
        this.route.parent
      );

      this.service.getAll(this.clusterName).subscribe(
        (workflows) => {
          console.log('workflows from workflow-detail.component', workflows);
          this.workflows = workflows;
          this.workflowRows = workflows.map((workflowName) => ({
            name: workflowName
          }));
          return workflows;
        },
        (error) => {
          // since rest API simply throws 404 instead of empty config when config is not initialized yet
          // frontend has to treat 404 as normal result
          if (error != 'Not Found') {
            console.error(error);
          }
          this.isLoading = false;
        },
        () => (this.isLoading = false)
      );
    }
  }

  onSelect({ selected }) {
    const row = selected[0];
    console.log('selected row', row)
  }
}
