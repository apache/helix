import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Resource } from '../shared/resource.model';

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

  resource: Resource;

  constructor(protected route: ActivatedRoute) { }

  ngOnInit() {
    this.resource = this.route.snapshot.data.resource;
  }

}
