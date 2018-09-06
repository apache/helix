import { Instance } from '../../instance/shared/instance.model';

export class Cluster {

  readonly name: string;
  readonly controller: string;
  readonly enabled: boolean;
  readonly instances: Instance[];
  readonly inMaintenance: boolean;

  // TODO vxu: Resources are useless here. Remove it please.
  readonly resources: string[];

  // TODO vxu: convert it to use StateModel[]
  readonly stateModels: string[];

  config: Object;

  constructor (obj: any) {
    this.name = obj.id;
    this.controller = obj.controller;
    this.enabled = !obj.paused;
    this.resources = obj.resources;
    this.inMaintenance = obj.maintenance;

    let ins: Instance[] = [];
    for (let instance of obj.instances) {
      ins.push(new Instance(
        instance,
        this.name,
        false, // here's a dummy value. should not be used
        obj.liveInstances.indexOf(instance) >= 0)
      );
    }
    this.instances = ins;
  }
}
