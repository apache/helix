import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Resource } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';

@Component({
  selector: 'hi-resource-list',
  templateUrl: './resource-list.component.html',
  styleUrls: ['./resource-list.component.scss'],
  providers: [ResourceService]
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
    private service: ResourceService
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
        this.route.parent.data.subscribe(data => {
          this.isLoading = true;
          this.clusterName = data.cluster.name;

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

  onSelect({ selected }) {
    let row = selected[0];

    if (this.isForInstance) {
      this.table.rowDetail.toggleExpandRow(row);
    } else {
      this.router.navigate(['/clusters', this.clusterName, 'resources', row.name]);
    }
  }

}
