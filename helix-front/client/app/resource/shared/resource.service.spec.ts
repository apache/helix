import { TestBed, inject } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { ResourceService } from './resource.service';

describe('ResourceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule],
      providers: [ResourceService],
    });
  });

  it('should be ready', inject(
    [ResourceService],
    (service: ResourceService) => {
      expect(service).toBeTruthy();
    }
  ));
});
