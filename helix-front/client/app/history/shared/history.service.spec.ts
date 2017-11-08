import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { HistoryService } from './history.service';

describe('HistoryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule, RouterTestingModule],
      providers: [HistoryService]
    });
  });

  it('should be ready', inject([HistoryService], (service: HistoryService) => {
    expect(service).toBeTruthy();
  }));
});
