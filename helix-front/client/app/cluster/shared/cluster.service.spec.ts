import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { ClusterService } from './cluster.service';

describe('ClusterService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule, RouterTestingModule],
      providers: [ClusterService]
    });
  });

  it('should ...', inject([ClusterService], (service: ClusterService) => {
    expect(service).toBeTruthy();
  }));
});
