import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { InstanceService } from './instance.service';

describe('InstanceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule, RouterTestingModule],
      providers: [InstanceService]
    });
  });

  it('should be created', inject([InstanceService], (service: InstanceService) => {
    expect(service).toBeTruthy();
  }));
});
