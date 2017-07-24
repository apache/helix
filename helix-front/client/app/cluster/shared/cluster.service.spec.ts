import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';

import { ClusterService } from './cluster.service';

describe('ClusterService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule],
      providers: [ClusterService]
    });
  });

  it('should ...', inject([ClusterService], (service: ClusterService) => {
    expect(service).toBeTruthy();
  }));
});
