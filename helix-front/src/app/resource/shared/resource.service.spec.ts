import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';

import { ResourceService } from './resource.service';

describe('ResourceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule],
      providers: [ResourceService]
    });
  });

  it('should be ready', inject([ResourceService], (service: ResourceService) => {
    expect(service).toBeTruthy();
  }));
});
