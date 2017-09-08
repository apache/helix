import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { InstanceService } from '../shared/instance.service';
import { HelperService } from '../../shared/helper.service';

@Component({
  selector: 'hi-instance-list',
  templateUrl: './instance-list.component.html',
  styleUrls: ['./instance-list.component.scss']
})
export class InstanceListComponent implements OnInit {

  isLoading = true;
  clusterName: string;
  instances: any[];
  rowHeight = 40;
  sorts = [
    { prop: 'liveInstance', dir: 'asc'},
    { prop: 'name', dir: 'asc'}
  ];

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected service: InstanceService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params['name'];
      this.service
        .getAll(this.clusterName)
        .subscribe(
          data => this.instances = data,
          error => this.helper.showError(error),
          () => this.isLoading = false
        );
    }
  }

  onSelect({ selected }) {
    let row = selected[0];
    this.router.navigate([row.name], { relativeTo: this.route });
  }

}
