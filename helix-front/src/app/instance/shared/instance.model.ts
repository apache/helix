export class Instance {

  readonly name: string;
  readonly clusterName: string;
  readonly liveInstance: boolean | string;
  readonly sessionId: string;
  readonly helixVersion: string;

  constructor(
    name: string,
    clusterName: string,
    liveInstance: boolean|string,
    sessionId?: string,
    helixVersion?: string
  ) {
    this.name = name;
    this.clusterName = clusterName;
    this.liveInstance = liveInstance;
    this.sessionId = sessionId;
    this.helixVersion = helixVersion;
  }
}
