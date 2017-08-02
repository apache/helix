import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { ResourceService } from './resource.service';

describe('ResourceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule, RouterTestingModule],
      providers: [ResourceService]
    });
  });

  it('should be ready', inject([ResourceService], (service: ResourceService) => {
    expect(service).toBeTruthy();
  }));
});
