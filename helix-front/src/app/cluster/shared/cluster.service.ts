import { map } from 'rxjs/operators';
import { Injectable } from '@angular/core';

import { Cluster } from './cluster.model';
import { HelixService } from '../../core/helix.service';

@Injectable()
export class ClusterService extends HelixService {
  public getAll() {
    return this.request('/clusters').pipe(
      map((data) => data.clusters.sort().map((name) => <Cluster>{ name }))
    );
  }

  public get(name: string) {
    return this.request(`/clusters/${name}`).pipe(
      map((data) => new Cluster(data))
    );
  }

  public create(name: string) {
    return this.put(`/clusters/${name}`, null);
  }

  public remove(name: string) {
    return this.delete(`/clusters/${name}`);
  }

  public enable(name: string) {
    return this.post(`/clusters/${name}?command=enable`, null);
  }

  public disable(name: string) {
    return this.post(`/clusters/${name}?command=disable`, null);
  }

  public activate(name: string, superCluster: string) {
    return this.post(
      `/clusters/${name}?command=activate&superCluster=${superCluster}`,
      null
    );
  }

  public enableMaintenanceMode(name: string, reason: string) {
    return this.post(`/clusters/${name}?command=enableMaintenanceMode`, {
      reason,
    });
  }

  public disableMaintenanceMode(name: string) {
    return this.post(`/clusters/${name}?command=disableMaintenanceMode`, null);
  }
}
