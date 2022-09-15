import { Injectable } from '@angular/core';
import { map } from 'rxjs/operators';

import { Job } from './workflow.model';
import { HelixService } from '../../core/helix.service';

@Injectable()
export class JobService extends HelixService {
  public get(job: Job) {
    return this.request(
      `/clusters/${job.clusterName}/workflows/${job.workflowName}/jobs/${job.rawName}`
    ).pipe(
      map((data) => {
        job.config = data.JobConfig;
        job.context = data.JobContext;
      })
    );
  }
}
