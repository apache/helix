import { TestBed, inject } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { TestingModule } from '../../../testing/testing.module';

import { WorkflowService } from './workflow.service';

describe('WorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      providers: [WorkflowService],
    });
  });

  it('should be created', inject(
    [WorkflowService],
    (service: WorkflowService) => {
      expect(service).toBeTruthy();
    }
  ));
});
