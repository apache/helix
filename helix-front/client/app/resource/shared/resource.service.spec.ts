import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';

import { ResourceService } from './resource.service';

describe('ResourceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [ResourceService]
    });
  });

  it('should be ready', inject([ResourceService], (service: ResourceService) => {
    expect(service).toBeTruthy();
  }));
});
