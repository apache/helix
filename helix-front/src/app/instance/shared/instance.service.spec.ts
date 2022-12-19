import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { InstanceService } from './instance.service';

describe('InstanceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [InstanceService],
    });
  });

  it('should be created', inject(
    [InstanceService],
    (service: InstanceService) => {
      expect(service).toBeTruthy();
    }
  ));
});
