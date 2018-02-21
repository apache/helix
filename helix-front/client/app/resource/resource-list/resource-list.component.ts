import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { Observable } from 'rxjs/Rx';

import * as _ from 'lodash';

import { Settings } from '../../core/settings';
import { Resource } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';
import { WorkflowService } from '../../workflow/shared/workflow.service';
import { HelperService } from '../../shared/helper.service';

@Component({
  selector: 'hi-resource-list',
  templateUrl: './resource-list.component.html',
  styleUrls: ['./resource-list.component.scss'],
  providers: [WorkflowService]
})
export class ResourceListComponent implements OnInit {

  @ViewChild('resourcesTable')
  table: any;

  isForInstance = false;
  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;
  resources: Resource[];
  isLoading = true;
  clusterName: string;
  instanceName: string;
  sorts = [
    { prop: 'alive', dir: 'asc'},
    { prop: 'name', dir: 'asc'}
  ];

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private service: ResourceService,
    private workflowService: WorkflowService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    if (this.route.parent) {

      if (this.route.snapshot.data.forInstance) {
        this.isForInstance = true;
        this.isLoading = true;
        this.clusterName = this.route.parent.snapshot.params.cluster_name;
        this.instanceName = this.route.parent.snapshot.params.instance_name;

        this.service
          .getAllOnInstance(this.clusterName, this.instanceName)
          .subscribe(
            resources => this.resources = resources,
            error => console.log(error),
            () => this.isLoading = false
          );
      } else {
        this.route.parent.params
          .map(p => p.name)
          .subscribe(name => {
            this.clusterName = name;
            this.fetchResources();
          });
      }
    }
  }

  // since resource list contains also workflows and jobs
  // need to subtract them from original resource list
  // to obtain all jobs list, need to go through every workflow
  // and perform get request for each.
  // However, it has huge performance issue when there are thousands of
  // workflows. We are using a smart way here: to remove resources whose
  // prefix is a workflow name
  protected fetchResources() {
    this.isLoading = true;
    this.resources = null;

    this.workflowService
      .getAll(this.clusterName)
      .subscribe(
        workflows => {
          this.service
            .getAll(this.clusterName)
            .subscribe(
              result => {
                this.resources = _.differenceWith(result, workflows, (resource: Resource, prefix: string) => _.startsWith(resource.name, prefix));
              },
              error => this.helper.showError(error),
              () => this.isLoading = false
            );
        },
        error => this.helper.showError(error)
      );
  }

  onSelect({ selected }) {
    const row = selected[0];

    if (this.isForInstance) {
      this.table.rowDetail.toggleExpandRow(row);
    } else {
      this.router.navigate([row.name], { relativeTo: this.route });
    }
  }

}
