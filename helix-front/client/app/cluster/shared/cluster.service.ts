import { Injectable } from '@angular/core';

import { Cluster } from './cluster.model';
import { HelixService } from '../../core/helix.service';

@Injectable()
export class ClusterService extends HelixService {

  public getAll() {
    return this
      .request('/clusters')
      .map(data => {
        return data
          .clusters
          .sort()
          .map(name => <Cluster>({name: name}));
      });
  }

  public get(name: string) {
    return this
      .request(`/clusters/${name}`)
      .map(data => {
        return new Cluster(data);
      });
  }
}
