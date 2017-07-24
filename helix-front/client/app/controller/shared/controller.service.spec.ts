import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';

import { ControllerService } from './controller.service';

describe('ControllerService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule],
      providers: [ControllerService]
    });
  });

  it('should be ready', inject([ControllerService], (service: ControllerService) => {
    expect(service).toBeTruthy();
  }));
});
