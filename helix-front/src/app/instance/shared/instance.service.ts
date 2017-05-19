import { Injectable } from '@angular/core';

import { Instance } from './instance.model';
import { HelixService } from '../../core/helix.service';

@Injectable()
export class InstanceService extends HelixService {

  public get(clusterName: string, instanceName: string) {
    return this
      .request(`/clusters/${ clusterName }/instances/${ instanceName }`)
      .map(data => {
        let liveInstance = data.liveInstance;

        return liveInstance && liveInstance.simpleFields ? new Instance(
          data.id,
          clusterName,
          liveInstance.simpleFields.LIVE_INSTANCE,
          liveInstance.simpleFields.SESSION_ID,
          liveInstance.simpleFields.HELIX_VERSION
        ) : new Instance(data.id, clusterName, false);
      });
  }

}
