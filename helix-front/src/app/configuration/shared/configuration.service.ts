import { Injectable } from '@angular/core';

import { HelixService } from '../../core/helix.service';

@Injectable()
export class ConfigurationService extends HelixService {

  public getClusterConfig(name: string) {
    return this.request(`/clusters/${ name }/configs`);
  }

  public getInstanceConfig(clusterName: string, instanceName: string) {
    return this.request(`/clusters/${ clusterName }/instances/${ instanceName }/configs`);
  }

  public getResourceConfig(clusterName: string, resourceName: string) {
    return this.request(`/clusters/${ clusterName }/resources/${ resourceName }/configs`);
  }
}
