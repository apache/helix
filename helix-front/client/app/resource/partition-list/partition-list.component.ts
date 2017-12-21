import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Settings } from '../../core/settings';
import { Partition, IReplica, Resource } from '../shared/resource.model';
import { HelperService } from '../../shared/helper.service';
import { ResourceService } from '../shared/resource.service';

@Component({
  selector: 'hi-partition-list',
  templateUrl: './partition-list.component.html',
  styleUrls: ['./partition-list.component.scss']
})
export class PartitionListComponent implements OnInit {

  @ViewChild('partitionsTable')
  table: any;

  isLoading = true;
  clusterName: string;
  resource: Resource;
  partitions: Partition[];
  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;
  sorts = [
    { prop: 'isReady', dir: 'asc'},
    { prop: 'name', dir: 'asc'}
  ];

  constructor(
    protected route: ActivatedRoute,
    protected service: ResourceService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params.cluster_name;

      this.loadResource();
    }
  }

  // check whether we are capable of analysing the states :(
  canAnalyse() {
    return this.partitions && this.partitions.length;
  }

  getReasonWhyCannotAnalyse(): boolean | string {
    if (!this.canAnalyse()) {
      if (!this.resource.online) {
        return 'The resource is OFFLINE and does not have partition information available.';
      }
      if (this.resource.partitionCount < 1) {
        return 'This resource does not contain any partition information.'
      }
    }

    return false;
  }

  onSelect({ selected }) {
    const row = selected[0];

    this.table.rowDetail.toggleExpandRow(row);
  }

  protected loadResource() {
    const resourceName = this.resource ? this.resource.name : this.route.parent.snapshot.params.resource_name;
    this.isLoading = true;
    this.service
      .get(this.clusterName, resourceName)
      .subscribe(
        resource => {
          this.resource = resource;
          this.partitions = this.resource.partitions;
        },
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
  }
}
