import { Injectable } from '@angular/core';

import { HelixService } from '../../core/helix.service';

@Injectable()
export class ChooserService extends HelixService {

  public getAll() {
    return this
      .request(`/list`, '');
  }
}
