import { map } from 'rxjs/operators';
import { Injectable } from '@angular/core';

import { Workflow } from './workflow.model';
import { HelixService } from '../../core/helix.service';

@Injectable()
export class WorkflowService extends HelixService {
  public getAll(clusterName: string) {
    return this.request(`/clusters/${clusterName}/workflows`).pipe(
      map((data) => data.Workflows.sort())
    );
  }

  public get(clusterName: string, workflowName: string) {
    return this.request(
      `/clusters/${clusterName}/workflows/${workflowName}`
    ).pipe(map((data) => new Workflow(data, clusterName)));
  }

  public stop(clusterName: string, workflowName: string) {
    return this.post(
      `/clusters/${clusterName}/workflows/${workflowName}?command=stop`,
      null
    );
  }

  public resume(clusterName: string, workflowName: string) {
    return this.post(
      `/clusters/${clusterName}/workflows/${workflowName}?command=resume`,
      null
    );
  }
}
