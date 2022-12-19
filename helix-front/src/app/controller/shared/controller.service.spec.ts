import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { ControllerService } from './controller.service';

describe('ControllerService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [ControllerService],
    });
  });

  it('should be ready', inject(
    [ControllerService],
    (service: ControllerService) => {
      expect(service).toBeTruthy();
    }
  ));
});
