import { Component, OnInit, Input } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

@Component({
  selector: 'hi-json-viewer',
  templateUrl: './json-viewer.component.html',
  styleUrls: ['./json-viewer.component.scss']
})
export class JsonViewerComponent implements OnInit {

  // MODE 1: use directly in components
  @Input() obj: any;

  constructor(protected route: ActivatedRoute) { }

  ngOnInit() {
    // MODE 2: use in router
    if (this.route.snapshot.data.path) {
      let path = this.route.snapshot.data.path;

      // try parent data first
      this.obj = _.get(this.route.parent, `snapshot.data.${ path }`);

      if (this.obj == null) {
        // try self data then
        this.obj = _.get(this.route.snapshot.data, path);
      }
    }
  }

}
