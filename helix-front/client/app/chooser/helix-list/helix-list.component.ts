import { Component, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { ChooserService } from '../shared/chooser.service';

@Component({
  selector: 'hi-helix-list',
  templateUrl: './helix-list.component.html',
  styleUrls: ['./helix-list.component.scss']
})
export class HelixListComponent implements OnInit {

  groups: any;
  keys = _.keys;

  constructor(
    protected service: ChooserService
  ) { }

  ngOnInit() {
    this.service.getAll()
      .subscribe(
        data => this.groups = data
      );
  }

}
