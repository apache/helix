export class Workflow {
  readonly name: String;
  readonly config: any;
  readonly jobs: string[];
  // TODO vxu: will use a better structure for this
  readonly parentJobs: any[];
  readonly context: any;

  constructor(obj: any) {
    this.name = obj.id;
    this.config = obj.WorkflowConfig;
    this.context = obj.WorkflowContext;
    this.jobs = obj.Jobs;
    this.parentJobs = obj.ParentJobs;
  }
}
