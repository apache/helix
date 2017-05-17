import { Injectable } from '@angular/core';

import { HelixService } from '../../core/helix.service';

@Injectable()
export class ConfigurationService extends HelixService {

  public getClusterConfig(name: string) {
    return this.request(`/clusters/${name}/configs`);
  }
}
