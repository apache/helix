import { Injectable } from '@angular/core';
import { map } from 'rxjs/operators';
import * as _ from 'lodash';

import { HelixService } from '../../core/helix.service';
import { History } from './history.model';

@Injectable()
export class HistoryService extends HelixService {
  getControllerHistory(clusterName: string) {
    return this.request(`/clusters/${clusterName}/controller/history`).pipe(
      map((data) => this.parseHistory(data.history))
    );
  }

  getInstanceHistory(clusterName: string, instanceName: string) {
    return this.request(
      `/clusters/${clusterName}/instances/${instanceName}/history`
    ).pipe(map((data) => this.parseHistory(data.listFields.HISTORY)));
    // TODO: implement data.simpleFields.LAST_OFFLINE_TIME
  }

  protected parseHistory(data: any): History[] {
    const histories: History[] = [];

    if (data) {
      for (const record of data) {
        // controller: {DATE=2017-04-13-22:33:55, CONTROLLER=ltx1-app1133.stg.linkedin.com_12923, TIME=1492122835198}
        // instance: {DATE=2017-05-01T08:21:42:114, SESSION=55a8e28052bcb56, TIME=1493626902114}
        const history = new History();

        for (const seg of _.words(record, /[^{}, ]+/g)) {
          const name = _.words(seg, /[^=]+/g)[0];
          const value = _.words(seg, /[^=]+/g)[1];
          if (name == 'DATE') {
            history.date = value;
          } else if (name == 'CONTROLLER') {
            history.controller = value;
          } else if (name == 'SESSION') {
            history.session = value;
          } else if (name == 'TIME') {
            history.time = +value;
          }
        }

        histories.push(history);
      }
    }

    return histories;
  }
}
