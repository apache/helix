export class Instance {

  readonly name: string;
  readonly clusterName: string;
  readonly enabled: boolean
  readonly liveInstance: boolean | string;
  readonly sessionId: string;
  readonly helixVersion: string;

  get healthy(): boolean {
    return this.liveInstance && this.enabled;
  }

  constructor(
    name: string,
    clusterName: string,
    enabled: boolean,
    liveInstance: boolean|string,
    sessionId?: string,
    helixVersion?: string
  ) {
    this.name = name;
    this.clusterName = clusterName;
    this.enabled = enabled;
    this.liveInstance = liveInstance;
    this.sessionId = sessionId;
    this.helixVersion = helixVersion;
  }
}
