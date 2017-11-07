import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

import { Resource } from '../shared/resource.model';
import { HelperService } from '../../shared/helper.service';
import { ResourceService } from '../shared/resource.service';

@Component({
  selector: 'hi-resource-node-viewer',
  templateUrl: './resource-node-viewer.component.html',
  styleUrls: ['./resource-node-viewer.component.scss']
})
export class ResourceNodeViewerComponent implements OnInit {

  isLoading = true;
  clusterName: string;
  resourceName: string;
  resource: Resource;
  path: string;
  obj: any;

  constructor(
    protected route: ActivatedRoute,
    protected service: ResourceService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    if (this.route.snapshot.data.path) {
      this.path = this.route.snapshot.data.path;
    }

    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params.cluster_name;
      this.resourceName = this.route.parent.snapshot.params.resource_name;

      this.loadResource();
    }
  }

  protected loadResource() {
    this.isLoading = true;
    this.service
      .get(this.clusterName, this.resourceName)
      .subscribe(
        resource => {
          this.resource = resource;
          this.obj = _.get(this.resource, this.path);
        },
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
  }
}
