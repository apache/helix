import { Component, OnInit, Input } from '@angular/core';

import { ResourceService } from '../shared/resource.service';

@Component({
  selector: 'hi-resource-detail-for-instance',
  templateUrl: './resource-detail-for-instance.component.html',
  styleUrls: ['./resource-detail-for-instance.component.scss']
})
export class ResourceDetailForInstanceComponent implements OnInit {

  @Input() clusterName;
  @Input() instanceName;
  @Input() resourceName;

  resourceOnInstance: any;
  isLoading = true;
  rowHeight = 40;
  sorts = [
    { prop: 'name', dir: 'asc'}
  ];

  constructor(protected service: ResourceService) { }

  ngOnInit() {
    this.service.getOnInstance(
      this.clusterName,
      this.instanceName,
      this.resourceName
    ).subscribe(
      resource => this.resourceOnInstance = resource,
      error => console.log(error),
      () => this.isLoading = false
    );
  }

}
