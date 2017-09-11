import * as _ from 'lodash';

export interface IReplica {
  instanceName: string;
  externalView: string;
  idealState: string;
}

export class Partition {
  name: string;
  replicas: IReplica[];

  get isReady() {
    return !_.some(this.replicas, replica => !replica.externalView || replica.externalView != replica.idealState);
  }

  constructor(name: string) {
    this.name = name;
    this.replicas = [];
  }
}

export class Resource {

  readonly name: string;

  // TODO vxu: convert it to an enum in future if necessary
  readonly alive: boolean;

  readonly cluster: string;

  // meta data
  readonly idealStateMode: string;
  readonly rebalanceMode: string;
  readonly stateModel: string;
  readonly partitionCount: number;
  readonly replicaCount: number;

  readonly idealState: any;
  readonly externalView: any;

  get enabled(): boolean {
    // there are two cases meaning enabled both:
    //   HELIX_ENABLED: true or no such item in idealState
    return _.get(this.idealState, 'simpleFields.HELIX_ENABLED') != 'false';
  }

  get online(): boolean {
     return !_.isEmpty(this.externalView);
  }

  readonly partitions: Partition[];

  constructor(cluster: string, name: string, config: any, idealState: any, externalView: any) {
    this.cluster = cluster;
    this.name = name;

    externalView = externalView || {};

    // ignore config for now since config component will fetch itself

    this.idealStateMode = idealState.simpleFields.IDEAL_STATE_MODE;
    this.rebalanceMode = idealState.simpleFields.REBALANCE_MODE;
    this.stateModel = idealState.simpleFields.STATE_MODEL_DEF_REF;
    this.partitionCount = +idealState.simpleFields.NUM_PARTITIONS;
    this.replicaCount = +idealState.simpleFields.REPLICAS;

    // fetch partition names from externalView.mapFields is (relatively) more stable
    this.partitions = [];
    for (let partitionName in externalView.mapFields) {
      let partition = new Partition(partitionName);

      // in FULL_ATUO mode, externalView is more important
      // if preferences list exists, fetch instances from it, else whatever
      if (this.rebalanceMode != 'FULL_AUTO' && idealState.listFields[partitionName]) {
        for (let replicaName of idealState.listFields[partitionName]) {
          partition.replicas.push(<IReplica>{
            instanceName: replicaName,
            externalView: _.get(externalView, ['mapFields', partitionName, replicaName]),
            idealState: _.get(idealState, ['mapFields', partitionName, replicaName])
          });
        }
      } else if (this.rebalanceMode != 'FULL_AUTO' && idealState.mapFields[partitionName]) {
        for (let replicaName in idealState.mapFields[partitionName]) {
          partition.replicas.push(<IReplica>{
            instanceName: replicaName,
            externalView: _.get(externalView, ['mapFields', partitionName, replicaName]),
            idealState: _.get(idealState, ['mapFields', partitionName, replicaName])
          });
        }
      } else {
        for (let replicaName in externalView.mapFields[partitionName]) {
          partition.replicas.push(<IReplica>{
            instanceName: replicaName,
            externalView: _.get(externalView, ['mapFields', partitionName, replicaName]),
            idealState: _.get(idealState, ['mapFields', partitionName, replicaName])
          });
        }
      }

      // sort replicas by states
      partition.replicas = _.sortBy(partition.replicas, 'externalView');

      this.partitions.push(partition);
    }

    this.idealState = idealState;
    this.externalView = externalView;
  }
}
