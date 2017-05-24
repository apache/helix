import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

@Component({
  selector: 'hi-instance-list',
  templateUrl: './instance-list.component.html',
  styleUrls: ['./instance-list.component.scss']
})
export class InstanceListComponent implements OnInit {

  instances: any[];
  rowHeight = 40;
  sorts = [
    { prop: 'liveInstance', dir: 'asc'},
    { prop: 'name', dir: 'asc'}
  ];

  constructor(
    private route: ActivatedRoute,
    private router: Router
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.route.parent.data.subscribe(
        data => this.instances = data.cluster.instances,
        error => console.log(error)
      );
    }
  }

  onSelect({ selected }) {
    let row = selected[0];
    this.router.navigate(['/clusters', row.clusterName, 'instances', row.name]);
  }

}
