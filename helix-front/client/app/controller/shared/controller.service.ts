import { Injectable } from '@angular/core';

import { HelixService } from '../../core/helix.service';
import { Controller } from './controller.model';

@Injectable()
export class ControllerService extends HelixService {

  public get(clusterName: string) {
    return this
      .request(`/clusters/${clusterName}/controller`)
      .map(data => {
        return new Controller(
          data.controller,
          clusterName,
          data.LIVE_INSTANCE,
          data.SESSION_ID,
          data.HELIX_VERSION
        );
      });
  }

}
