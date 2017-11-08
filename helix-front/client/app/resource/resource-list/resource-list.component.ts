import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Resource } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';
import { WorkflowService } from '../../workflow/shared/workflow.service';
import { HelperService } from '../../shared/helper.service';
import { Observable } from 'rxjs/Rx';
import * as _ from 'lodash';

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
  rowHeight = 40;
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
  // and perform get request for each
  protected fetchResources() {
    let jobs = [];

    this.isLoading = true;
    this.resources = null;

    this.workflowService
      .getAll(this.clusterName)
      .concatMap(workflows => Observable.from(workflows))
      .mergeMap(workflow => {
        const name = workflow as string;
        jobs.push(name);
        return this.workflowService.get(this.clusterName, name);
      })
      .map(workflow => workflow.jobs)
      .subscribe(
        list => {
          jobs = jobs.concat(list);
        },
        error => this.helper.showError(error),
        () => {
          this.service
            .getAll(this.clusterName)
            .subscribe(
              result => {
                this.resources = _.differenceWith(result, jobs, (resource: Resource, name) => resource.name === name);
              },
              error => this.helper.showError(error),
              () => this.isLoading = false
            );
        }
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
