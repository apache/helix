import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Cluster } from '../shared/cluster.model';

@Component({
  selector: 'hi-cluster-detail',
  templateUrl: './cluster-detail.component.html',
  styleUrls: ['./cluster-detail.component.scss']
})
export class ClusterDetailComponent implements OnInit {

  readonly tabLinks = [
    { label: 'Resources', link: 'resources' },
    { label: 'Workflows', link: 'workflows' },
    { label: 'Instances', link: 'instances' },
    { label: 'Configuration', link: 'configs' }
  ];

  cluster: Cluster;

  constructor(private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.route.data.subscribe(data => this.cluster = data.cluster);

    this.route.params
      .map(p => p.name)
      .subscribe(name => {
        console.log(name);
      });
  }

}
