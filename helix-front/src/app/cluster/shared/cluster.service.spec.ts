import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { ClusterService } from './cluster.service';

describe('ClusterService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [ClusterService],
    });
  });

  it('should ...', inject([ClusterService], (service: ClusterService) => {
    expect(service).toBeTruthy();
  }));
});
