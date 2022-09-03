import { TestBed, inject } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientModule } from '@angular/common/http';

import { ChooserService } from './chooser.service';

describe('ChooserService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule, HttpClientModule],
      providers: [ChooserService],
    });
  });

  it('should be created', inject(
    [ChooserService],
    (service: ChooserService) => {
      expect(service).toBeTruthy();
    }
  ));
});
