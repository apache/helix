import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';

import { ConfigurationService } from './configuration.service';

describe('ConfigurationService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule],
      providers: [ConfigurationService]
    });
  });

  it('should be ready', inject([ConfigurationService], (service: ConfigurationService) => {
    expect(service).toBeTruthy();
  }));
});
