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
  clusterName: string;

  constructor(
    private route: ActivatedRoute,
    private service: ResourceService
  ) { }

  ngOnInit() {
    if (this.route.parent) {

      if (this.route.snapshot.data.forInstance) {
        this.isLoading = true;
        this.clusterName = this.route.parent.snapshot.params.cluster_name;

        this.service.getAllOnInstance(
          this.route.parent.snapshot.params.cluster_name,
          this.route.parent.snapshot.params.instance_name
        ).subscribe(
          resources => this.resources = resources,
          error => console.log(error),
          () => this.isLoading = false
        );
      } else {
        this.clusterName = this.route.parent.snapshot.params.name;

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

}
