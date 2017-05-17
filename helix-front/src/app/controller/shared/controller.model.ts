export class Controller {
  readonly name: string;
  readonly clusterName: string;
  readonly liveInstance: string;
  readonly sessionId: string;
  readonly helixVersion: string;

  constructor(name, cluster, liveInstance, sessionId, version) {
    this.name = name;
    this.clusterName = cluster;
    this.liveInstance = liveInstance;
    this.sessionId = sessionId;
    this.helixVersion = version;
  }
}
