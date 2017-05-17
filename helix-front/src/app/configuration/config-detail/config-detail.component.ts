import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { ConfigurationService } from '../shared/configuration.service';

import * as _ from 'lodash';

@Component({
  selector: 'hi-config-detail',
  templateUrl: './config-detail.component.html',
  styleUrls: ['./config-detail.component.scss'],
  providers: [ConfigurationService]
  // no need to customize ViewEncapsulation for now
  // // Since we are importing external styles in this component
  // // we will not use Shadow DOM at all to make sure the styles apply
  // encapsulation: ViewEncapsulation.None
})
export class ConfigDetailComponent implements OnInit {

  isLoading = true;
  rowHeight = 40;
  sorts = [
    { prop: 'name', dir: 'asc'}
  ];
  keyword = '';

  _simpleConfigs: any[];
  get simpleConfigs(): any[] {
    return _.filter(this._simpleConfigs, config => {
      return config.name.toLowerCase().indexOf(this.keyword) >= 0
        || config.value.toLowerCase().indexOf(this.keyword) >=0;
    });
  }

  _listConfigs: any[];
  get listConfigs(): any[] {
    return _.filter(this._listConfigs, config => {
      return config.name.toLowerCase().indexOf(this.keyword) >= 0
        || _.some(config.value as any[], subconfig => {
          return subconfig.value.toLowerCase().indexOf(this.keyword) >= 0;
        });
    });
  }

  _mapConfigs: any[];
  get mapConfigs(): any[] {
    return _.filter(this._mapConfigs, config => {
      return config.name.toLowerCase().indexOf(this.keyword) >= 0
        || _.some(config.value as any[], subconfig => {
          return subconfig.name.toLowerCase().indexOf(this.keyword) >= 0
            || subconfig.value.toLowerCase().indexOf(this.keyword) >=0;
        });
    });
  }

  constructor(
    private route: ActivatedRoute,
    private serivce: ConfigurationService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.route.parent.data
        .subscribe(data => {
          this.isLoading = true;

          this.serivce
            .getClusterConfig(data.cluster.name)
            .subscribe(
              config => this.parseConfigs(config),
              error => {},
              () => this.isLoading = false
            );
        });
    }
  }

  updateFilter(event) {
    this.keyword = event.target.value.toLowerCase().trim();
  }

  protected parseConfigs(value) {
    if (value) {
      this._simpleConfigs = this.keyValueToArray(value['simpleFields']);

      this._listConfigs = [];
      _.forOwn(value['listFields'], (v, k) => {
        this._listConfigs.push({
          name: k,
          value: _.map(v, item => {
            return {
              value: item
            }
          })
        });
      });

      this._mapConfigs = [];
      _.forOwn(value['mapFields'], (v, k) => {
        this._mapConfigs.push({
          name: k,
          value: this.keyValueToArray(v)
        });
      });
    }
  }

  // Converting simpleFields to format like rows
  private keyValueToArray(obj: Object): any[] {
    let result = [];
    for (let k in obj) {
      result.push({
        name: k,
        value: obj[k]
      })
    }
    return result;
  }
}
