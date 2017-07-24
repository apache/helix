import { Component, OnInit, Input, ViewChild, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

import { Node } from '../models/node.model';
import { Settings } from '../../core/settings';

@Component({
  selector: 'hi-node-viewer',
  templateUrl: './node-viewer.component.html',
  styleUrls: ['./node-viewer.component.scss'],
  // Since we are importing external styles in this component
  // we will not use Shadow DOM at all to make sure the styles apply
  encapsulation: ViewEncapsulation.None
})
export class NodeViewerComponent implements OnInit {

  @ViewChild('simpleTable') simpleTable;
  @ViewChild('listTable') listTable;
  @ViewChild('mapTable') mapTable;

  // MODE 1: use directly in components
  @Input()
  set obj(value: any) {
    if (value != null) {
      this._obj = value;
      this.node = new Node(value);
    }
  }
  get obj(): any {
    return this._obj;
  }

  protected _obj: any;
  protected node: Node;

  rowHeight = Settings.tableRowHeight;
  sorts = [
    { prop: 'name', dir: 'asc'}
  ];
  keyword = '';

  _simpleConfigs: any[];
  get simpleConfigs(): any[] {
    return this.node ? _.filter(this.node.simpleFields, config => {
      return config.name.toLowerCase().indexOf(this.keyword) >= 0
        || config.value.toLowerCase().indexOf(this.keyword) >= 0;
    }) : [];
  }

  _listConfigs: any[];
  get listConfigs(): any[] {
    return this.node ?  _.filter(this.node.listFields, config => {
      return config.name.toLowerCase().indexOf(this.keyword) >= 0
        || _.some(config.value as any[], subconfig => {
          return subconfig.value.toLowerCase().indexOf(this.keyword) >= 0;
        });
    }) : [];
  }

  _mapConfigs: any[];
  get mapConfigs(): any[] {
    return this.node ?  _.filter(this.node.mapFields, config => {
      return config.name.toLowerCase().indexOf(this.keyword) >= 0
        || _.some(config.value as any[], subconfig => {
          return subconfig.name.toLowerCase().indexOf(this.keyword) >= 0
            || subconfig.value.toLowerCase().indexOf(this.keyword) >= 0;
        });
    }) : [];
  }

  constructor(protected route: ActivatedRoute) { }

  ngOnInit() {
    // MODE 2: use in router
    if (this.route.snapshot.data.path) {
      const path = this.route.snapshot.data.path;

      // try parent data first
      this.obj = _.get(this.route.parent, `snapshot.data.${ path }`);

      if (this.obj == null) {
        // try self data then
        this.obj = _.get(this.route.snapshot.data, path);
      }
    }
  }

  updateFilter(event) {
    this.keyword = event.target.value.toLowerCase().trim();

    // Whenever the filter changes, always go back to the first page
    if (this.simpleTable) {
      this.simpleTable.offset = 0;
    }
    if (this.listTable) {
      this.listTable.offset = 0;
    }
    if (this.mapTable) {
      this.mapTable.offset = 0;
    }
  }

  getNameCellClass({ value }): any {
    return {
      // highlight HELIX own configs
      'primary': _.snakeCase(value).toUpperCase() === value
    };
  }

}
