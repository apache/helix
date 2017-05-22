import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { HelixService } from '../../core/helix.service';
import { Resource } from './resource.model';
import { Cluster } from '../../cluster/shared/cluster.model';

@Injectable()
export class ResourceService extends HelixService {

  public getAll(clusterName: string) {
    return this
      .request(`/clusters/${ clusterName }/resources`)
      .map(data => {
        let res: Resource[] = [];
        for (let name of data.idealStates) {
          res.push(<Resource>({
            cluster: clusterName,
            name: name,
            alive: data.externalViews.indexOf(name) >= 0
          }));
        }
        return _.sortBy(res, 'name');
      });
  }

  public getAllOnInstance(clusterName: string, instanceName: string) {
    return this
      .request(`/clusters/${ clusterName }/instances/${ instanceName }/resources`)
      .map(data => {
        let res: any[] = [];
        if (data) {
          for (let resource of data.resources) {
            res.push({
              name: resource
            });
          }
        }
        return res;
      });
  }

  public get(clusterName: string, resourceName: string) {
    return this
      .request(`/clusters/${ clusterName }/resources/${ resourceName }`)
      .map(data => {
        return new Resource(
          clusterName,
          resourceName,
          data.resourceConfig,
          data.idealState,
          data.externalView
        );
      });
  }
}
