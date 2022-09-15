import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';

import { HistoryService } from './history.service';

describe('HistoryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [HistoryService],
    });
  });

  it('should be ready', inject([HistoryService], (service: HistoryService) => {
    expect(service).toBeTruthy();
  }));
});
