import * as _ from 'lodash';

export class Task {

}

export class Job {
  readonly name: string;
  readonly rawName: string;
  readonly startTime: string;
  readonly state: string;
  readonly parents: string[];

  constructor(
    rawName: string,
    workflowName: string,
    startTime: string,
    state: string,
    parents: string[]
  ) {
    this.rawName = rawName;
    // try to reduce the name
    this.name = _.replace(rawName, workflowName + '_', '');
    this.startTime = startTime;
    this.state = state;
    // try to reduce parent names
    this.parents = _.map(parents, parent => _.replace(parent, workflowName + '_', ''));
  }
}

export class Workflow {
  readonly name: string;
  readonly config: any;
  readonly jobs: Job[];
  readonly context: any;
  readonly json: any;

  get isJobQueue(): boolean {
    return this.config && this.config.IsJobQueue.toLowerCase() == 'true';
  }

  get state(): string {
    return this.context.STATE || 'NOT STARTED';
  }

  constructor(obj: any) {
    this.json = obj;
    this.name = obj.id;
    this.config = obj.WorkflowConfig;
    this.context = obj.WorkflowContext;
    this.jobs = this.parseJobs(obj.Jobs, obj.ParentJobs);
  }

  protected parseJobs(list: string[], parents: any): Job[] {
    let result: Job[] = [];

    _.forEach(list, jobName => {
      result.push(new Job(
        jobName,
        this.name,
        _.get(this.context, ['StartTime', jobName]),
        _.get(this.context, ['JOB_STATES', jobName]),
        parents[jobName]
      ));
    });

    return result;
  }
}
