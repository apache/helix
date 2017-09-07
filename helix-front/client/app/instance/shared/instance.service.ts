import { Injectable } from '@angular/core';

import { Instance } from './instance.model';
import { HelixService } from '../../core/helix.service';
import { Node } from '../../shared/models/node.model';

@Injectable()
export class InstanceService extends HelixService {

  public getAll(clusterName: string) {
    return this
      .request(`/clusters/${ clusterName }/instances`)
      .map(data => {
        const onlineInstances = data.online;
        const disabledInstances = data.disabled;

        return data
          .instances
          .sort()
          .map(name => new Instance(
            name,
            clusterName,
            disabledInstances.indexOf(name) < 0,
            onlineInstances.indexOf(name) >= 0
          ));
      });
  }

  public get(clusterName: string, instanceName: string) {
    return this
      .request(`/clusters/${ clusterName }/instances/${ instanceName }`)
      .map(data => {
        const liveInstance = data.liveInstance;
        const config = data.config;
        const enabled = config && config.simpleFields && config.simpleFields.HELIX_ENABLED == 'true';

        return liveInstance && liveInstance.simpleFields ? new Instance(
          data.id,
          clusterName,
          enabled,
          liveInstance.simpleFields.LIVE_INSTANCE,
          liveInstance.simpleFields.SESSION_ID,
          liveInstance.simpleFields.HELIX_VERSION
        ) : new Instance(data.id, clusterName, enabled, null);
      });
  }

  public create(clusterName: string, host: string, port: string, enabled: boolean) {
    const name = `${ host }_${ port }`;

    let node = new Node(null);
    node.appendSimpleField('HELIX_ENABLED', enabled ? 'true' : 'false');
    node.appendSimpleField('HELIX_HOST', host);
    node.appendSimpleField('HELIX_PORT', port);

    return this
      .put(`/clusters/${ clusterName }/instances/${ name }`, node.json(name));
  }

  public remove(clusterName: string, instanceName: string) {
    return this
      .delete(`/clusters/${ clusterName }/instances/${ instanceName }`);
  }

  public enable(clusterName: string, instanceName: string) {
    return this
      .post(`/clusters/${ clusterName }/instances/${ instanceName }?command=enable`, null);
  }

  public disable(clusterName: string, instanceName: string) {
    return this
      .post(`/clusters/${ clusterName }/instances/${ instanceName }?command=disable`, null);
  }
}
