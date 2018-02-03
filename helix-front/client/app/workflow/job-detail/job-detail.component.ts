import { Component, OnInit, Input } from '@angular/core';

import { Job } from '../shared/workflow.model';
import { JobService } from '../shared/job.service';
import { HelperService } from '../../shared/helper.service';

@Component({
  selector: 'hi-job-detail',
  templateUrl: './job-detail.component.html',
  styleUrls: ['./job-detail.component.scss']
})
export class JobDetailComponent implements OnInit {

  @Input()
  job: Job;

  isLoading = true;

  constructor(
    protected service: JobService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    this.service.get(this.job)
      .subscribe(
        data => this.isLoading = false,
        error => this.helper.showError(error),
        () => this.isLoading = false
      )
  }

}
