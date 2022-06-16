import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';

import { HelixService } from './helix.service';

describe('HelixService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [HelixService],
    });
  });

  it('should be ready', inject([HelixService], (service: HelixService) => {
    expect(service).toBeTruthy();
  }));
});
