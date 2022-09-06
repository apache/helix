import { TestBed, inject } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientModule } from '@angular/common/http';

import { ConfigurationService } from './configuration.service';

describe('ConfigurationService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [ConfigurationService],
    });
  });

  it('should be ready', inject(
    [ConfigurationService],
    (service: ConfigurationService) => {
      expect(service).toBeTruthy();
    }
  ));
});
