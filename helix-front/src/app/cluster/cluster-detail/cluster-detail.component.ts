import { Component, OnInit } from '@angular/core';
import {
  Router,
  ActivatedRoute,
  NavigationStart,
  NavigationEnd,
  NavigationCancel,
  NavigationError
} from '@angular/router';

import { Cluster } from '../shared/cluster.model';

@Component({
  selector: 'hi-cluster-detail',
  templateUrl: './cluster-detail.component.html',
  styleUrls: ['./cluster-detail.component.scss']
})
export class ClusterDetailComponent implements OnInit {

  readonly tabLinks = [
    { label: 'Resources', link: 'resources' },
    { label: 'Instances', link: 'instances' },
    { label: 'Configuration', link: 'config' }
  ];

  cluster: Cluster;
  errorMessage: string;
  isLoading: boolean;

  constructor(
    private route: ActivatedRoute,
    private router: Router
  ) {
    router.events.subscribe(event => {
      if (event instanceof NavigationStart) {
        this.isLoading = true;
      }
      if (event instanceof NavigationEnd) {
        this.isLoading = false;
      }
      if (event instanceof NavigationError) {
        this.isLoading = false;
      }
      if (event instanceof NavigationCancel) {
        this.isLoading = false;
      }
    });
  }

  ngOnInit() {
    this.route.data.subscribe(data => this.cluster = data.cluster);
  }

}
