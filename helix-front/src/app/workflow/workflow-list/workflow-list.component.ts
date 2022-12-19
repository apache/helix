import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Settings } from '../../core/settings';
import { WorkflowService } from '../shared/workflow.service';
import { DatatableComponent } from '@swimlane/ngx-datatable';

type WorkflowRow = {
  name: string;
};

@Component({
  selector: 'hi-workflow-list',
  templateUrl: './workflow-list.component.html',
  styleUrls: ['./workflow-list.component.scss'],
})
export class WorkflowListComponent implements OnInit {
  @ViewChild('workflowsTable', { static: false })
  table: DatatableComponent;

  isLoading = true;
  clusterName: string;
  workflowRows: WorkflowRow[];

  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;

  sorts = [{ prop: 'name', dir: 'asc' }];

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private workflowService: WorkflowService
  ) {}

  ngOnInit() {
    if (this.route.parent) {
      this.isLoading = true;
      this.clusterName = this.route.parent.snapshot.params['name'];

      this.workflowService.getAll(this.clusterName).subscribe(
        (workflows) => {
          this.workflowRows = workflows.map((workflowName) => {
            return {
              name: workflowName,
            };
          });
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

  // Disable table row selection using the
  // selectCheck option of the
  // <ngx-datatable></ngx-datatable> element
  checkSelectable(_event) {
    return false;
  }
}
