import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { WorkflowService } from './workflow.service';

describe('WorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule, RouterTestingModule],
      providers: [WorkflowService]
    });
  });

  it('should be created', inject([WorkflowService], (service: WorkflowService) => {
    expect(service).toBeTruthy();
  }));
});
