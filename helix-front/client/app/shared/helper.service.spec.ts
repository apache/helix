import { TestBed, inject } from '@angular/core/testing';

import { HelperService } from './helper.service';
import { TestingModule } from '../../testing/testing.module';

describe('HelperService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [ TestingModule ],
      providers: [ HelperService ]
    });
  });

  it('should be created', inject([HelperService], (service: HelperService) => {
    expect(service).toBeTruthy();
  }));
});
