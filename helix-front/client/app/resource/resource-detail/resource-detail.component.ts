import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Resource } from '../shared/resource.model';
import { HelperService } from '../../shared/helper.service';
import { ResourceService } from '../shared/resource.service';

@Component({
  selector: 'hi-resource-detail',
  templateUrl: './resource-detail.component.html',
  styleUrls: ['./resource-detail.component.scss']
})
export class ResourceDetailComponent implements OnInit {

  readonly tabLinks = [
    { label: 'Partitions', link: 'partitions'},
    { label: 'External View', link: 'externalView'},
    { label: 'Ideal State', link: 'idealState'},
    { label: 'Configuration', link: 'configs' }
  ];

  clusterName: string;
  resourceName: string;
  resource: Resource;
  isLoading = true;
  can = false;

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected service: ResourceService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    this.service.can().subscribe(data => this.can = data);
    this.clusterName = this.route.snapshot.params.cluster_name;
    this.resourceName = this.route.snapshot.params.resource_name;
    this.loadResource();
  }

  enableResource() {
    this.service
      .enable(this.clusterName, this.resource.name)
      .subscribe(
        () => this.loadResource(),
        error => this.helper.showError(error)
      );
  }

  disableResource() {
    this.service
      .disable(this.clusterName, this.resource.name)
      .subscribe(
        () => this.loadResource(),
        error => this.helper.showError(error)
      );
  }

  removeResource() {
    this.helper
      .showConfirmation('Are you sure you want to remove this Resource?')
      .then(result => {
        console.log(result);
        if (result) {
          this.service
            .remove(this.clusterName, this.resourceName)
            .subscribe(data => {
              this.helper.showSnackBar(`Resource: ${ this.resourceName } removed!`);
              this.router.navigate(['..'], { relativeTo: this.route });
            });
        }
      });
  }

  protected loadResource() {
    this.isLoading = true;
    this.service
      .get(this.clusterName, this.resourceName)
      .subscribe(
        resource => this.resource = resource,
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
  }
}
