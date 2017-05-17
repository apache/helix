export class Instance {

  readonly name: string;
  // TODO vxu: convert it to an enum in future if necessary
  readonly alive: boolean;

  constructor(name: string, alive: boolean) {
    this.name = name;
    this.alive = alive;
  }
}
