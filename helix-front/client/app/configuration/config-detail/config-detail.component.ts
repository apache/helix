import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { ConfigurationService } from '../shared/configuration.service';
import { HelperService } from '../../shared/helper.service';

@Component({
  selector: 'hi-config-detail',
  templateUrl: './config-detail.component.html',
  styleUrls: ['./config-detail.component.scss'],
  providers: [ConfigurationService]
})
export class ConfigDetailComponent implements OnInit {

  isLoading = true;
  obj: any = {};
  clusterName: string;
  instanceName: string;
  resourceName: string;
  can = false;

  constructor(
    protected route: ActivatedRoute,
    protected service: ConfigurationService,
    protected helper: HelperService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params.name || this.route.parent.snapshot.params.cluster_name;
      this.instanceName = this.route.parent.snapshot.params.instance_name;
      this.resourceName = this.route.parent.snapshot.params.resource_name;
    }

    this.loadConfig();

    this.service.can().subscribe(data => this.can = data);
  }

  loadConfig() {
    let observer: any;

    if (this.clusterName && this.instanceName) {
      observer = this.service.getInstanceConfig(this.clusterName, this.instanceName);
    } else if (this.clusterName && this.resourceName) {
      observer = this.service.getResourceConfig(this.clusterName, this.resourceName);
    } else {
      observer = this.service.getClusterConfig(this.clusterName);
    }

    if (observer) {
      this.isLoading = true;
      observer.subscribe(
        config => this.obj = config,
        error => {
          // since rest API simply throws 404 instead of empty config when config is not initialized yet
          // frontend has to treat 404 as normal result
          if (error != 'Not Found') {
            this.helper.showError(error);
          }
          this.isLoading = false;
        },
        () => this.isLoading = false
      );
    }
  }

  updateConfig(value: any) {
    let observer: any;

    if (this.clusterName && this.instanceName) {
      observer = this.service.setInstanceConfig(this.clusterName, this.instanceName, value);
    } else if (this.clusterName && this.resourceName) {
      observer = this.service.setResourceConfig(this.clusterName, this.resourceName, value);
    } else {
      observer = this.service.setClusterConfig(this.clusterName, value);
    }

    if (observer) {
      this.isLoading = true;
      observer.subscribe(
        () => {
          this.helper.showSnackBar('Configuration updated!');
          this.loadConfig();
        },
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
    }
  }

  deleteConfig(value: any) {
    let observer: any;

    if (this.clusterName && this.instanceName) {
      observer = this.service.deleteInstanceConfig(this.clusterName, this.instanceName, value);
    } else if (this.clusterName && this.resourceName) {
      observer = this.service.deleteResourceConfig(this.clusterName, this.resourceName, value);
    } else {
      observer = this.service.deleteClusterConfig(this.clusterName, value);
    }

    if (observer) {
      this.isLoading = true;
      observer.subscribe(
        () => {
          this.helper.showSnackBar('Configuration deleted!');
          this.loadConfig();
        },
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
    }
  }
}
