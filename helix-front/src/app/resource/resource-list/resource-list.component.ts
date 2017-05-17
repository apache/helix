import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Resource } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';

@Component({
  selector: 'hi-resource-list',
  templateUrl: './resource-list.component.html',
  styleUrls: ['./resource-list.component.scss'],
  providers: [ResourceService]
})
export class ResourceListComponent implements OnInit {

  resources: Resource[];
  isLoading = true;

  constructor(
    private route: ActivatedRoute,
    private service: ResourceService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.route.parent.data.subscribe(data => {
        this.isLoading = true;

        this.service
          .getAll(data.cluster.name)
          .subscribe(
            resources => this.resources = resources,
            error => {},
            () => this.isLoading = false
          );
      });
    }
  }

}
