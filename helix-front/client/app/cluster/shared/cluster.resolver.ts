import { Injectable } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot } from '@angular/router';

import { ClusterService } from './cluster.service';
import { Cluster } from './cluster.model';

@Injectable()
export class ClusterResolver implements Resolve<Cluster> {

  constructor(private clusterService: ClusterService) {}

  resolve(route: ActivatedRouteSnapshot) {
    return this.clusterService.get(route.paramMap.get('name'));
  }
}
