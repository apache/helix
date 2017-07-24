import { TestBed, inject } from '@angular/core/testing';
import { HttpModule } from '@angular/http';

import { HistoryService } from './history.service';

describe('HistoryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpModule],
      providers: [HistoryService]
    });
  });

  it('should be ready', inject([HistoryService], (service: HistoryService) => {
    expect(service).toBeTruthy();
  }));
});
