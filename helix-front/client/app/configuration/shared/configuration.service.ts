import { Injectable } from '@angular/core';

import { HelixService } from '../../core/helix.service';
import { Node } from '../../shared/models/node.model';

@Injectable()
export class ConfigurationService extends HelixService {

  public getClusterConfig(name: string) {
    return this.request(`/clusters/${ name }/configs`);
  }

  public setClusterConfig(name: string, config: Node) {
    return this.post(`/clusters/${ name }/configs?command=update`, config.json(name));
  }

  public deleteClusterConfig(name: string, config: Node) {
    return this.post(`/clusters/${ name }/configs?command=delete`, config.json(name));
  }


  public getInstanceConfig(clusterName: string, instanceName: string) {
    return this.request(`/clusters/${ clusterName }/instances/${ instanceName }/configs`);
  }

  public setInstanceConfig(clusterName: string, instanceName: string, config: Node) {
    return this.post(`/clusters/${ clusterName }/instances/${ instanceName }/configs?command=update`, config.json(instanceName));
  }

  public deleteInstanceConfig(clusterName: string, instanceName: string, config: Node) {
    return this.post(`/clusters/${ clusterName }/instances/${ instanceName }/configs?command=delete`, config.json(instanceName));
  }


  public getResourceConfig(clusterName: string, resourceName: string) {
    return this.request(`/clusters/${ clusterName }/resources/${ resourceName }/configs`);
  }

  public setResourceConfig(clusterName: string, resourceName: string, config: Node) {
    return this.post(`/clusters/${ clusterName }/resources/${ resourceName }/configs?command=update`, config.json(resourceName));
  }

  public deleteResourceConfig(clusterName: string, resourceName: string, config: Node) {
    return this.post(`/clusters/${ clusterName }/resources/${ resourceName }/configs?command=delete`, config.json(resourceName));
  }
}
