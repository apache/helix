import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

import { HistoryService } from '../shared/history.service';
import { History } from '../shared/history.model';

@Component({
  selector: 'hi-history-list',
  templateUrl: './history-list.component.html',
  styleUrls: ['./history-list.component.scss'],
  providers: [HistoryService],
  // FIXME: have to turn off shadow dom or .current-controller won't work
  encapsulation: ViewEncapsulation.None
})
export class HistoryListComponent implements OnInit {

  rows: History[];
  rowHeight = 40;
  isController: boolean;
  isLoading = true;
  sorts = [
    { prop: 'date', dir: 'desc'}
  ];

  // to let ngx-datatable helper funcs have 'this' context
  bindFunc = _.bind;

  constructor(
    private route: ActivatedRoute,
    private service: HistoryService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      let clusterName = this.route.parent.snapshot.params['cluster_name'];
      let instanceName = this.route.parent.snapshot.params['instance_name'];
      let observable = instanceName
        ? this.service.getInstanceHistory(clusterName, instanceName)
        : this.service.getControllerHistory(clusterName);

      this.isController = !instanceName;

      observable.subscribe(
        histories => this.rows = histories,
        error => {},
        () => this.isLoading = false
      );
    }
  }

  getControllerCellClass({ value }): any {
    return {
      'current': value == this.rows[this.rows.length - 1].controller
    };
  }

  getSessionCellClass({ value }): any {
    return {
      'current': value == this.rows[this.rows.length - 1].session
    };
  }

}
