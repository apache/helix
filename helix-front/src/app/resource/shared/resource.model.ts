interface IReplica {
  instanceName: string;
  externalView: string;
  idealState: string;
}

interface IPartition {
  name: string;
  replicas: IReplica[];
}

export class Resource {

  readonly name: string;

  // TODO vxu: convert it to an enum in future if necessary
  readonly alive: boolean;

  readonly cluster: string;

  constructor(cluster: string, name: string, config: any, idealState: any, externalView: any) {
    this.cluster = cluster;
    this.name = name;

    // TODO: impl
  }
}
