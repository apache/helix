import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { HelixService } from './helix.service';

describe('HelixService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule, RouterTestingModule],
      providers: [HelixService]
    });
  });

  it('should be ready', inject([HelixService], (service: HelixService) => {
    expect(service).toBeTruthy();
  }));
});
