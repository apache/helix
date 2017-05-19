import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';

import { InstanceService } from './instance.service';

describe('InstanceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule],
      providers: [InstanceService]
    });
  });

  it('should be created', inject([InstanceService], (service: InstanceService) => {
    expect(service).toBeTruthy();
  }));
});
