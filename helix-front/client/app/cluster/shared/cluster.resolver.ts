import { Injectable } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot } from '@angular/router';

import { ClusterService } from './cluster.service';
import { Cluster } from './cluster.model';

/* not using this resolver for now since it will break the page when reload the page */

@Injectable()
export class ClusterResolver implements Resolve<Cluster> {

  constructor(private clusterService: ClusterService) {}

  resolve(route: ActivatedRouteSnapshot) {
    return this.clusterService.get(route.paramMap.get('name'));
  }
}
