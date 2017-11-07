import { Injectable } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot } from '@angular/router';

import { ResourceService } from './resource.service';
import { Resource } from './resource.model';

@Injectable()
export class ResourceResolver implements Resolve<Resource> {

  constructor(private service: ResourceService) {}

  resolve(route: ActivatedRouteSnapshot) {
    return this.service.get(
      route.paramMap.get('cluster_name'),
      route.paramMap.get('resource_name')
    );
  }
}
