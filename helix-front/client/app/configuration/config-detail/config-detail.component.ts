import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { MdSnackBar } from '@angular/material';

import { ConfigurationService } from '../shared/configuration.service';

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

  constructor(
    protected route: ActivatedRoute,
    protected service: ConfigurationService,
    protected snackBar: MdSnackBar
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      // TODO vxu: convert this logic to config.resolver
      if (this.route.snapshot.data.forInstance) {
        this.isLoading = true;

        this.service
          .getInstanceConfig(
            this.route.parent.snapshot.params.cluster_name,
            this.route.parent.snapshot.params.instance_name
          )
          .subscribe(
            config => this.obj = config,
            error => this.handleError(error),
            () => this.isLoading = false
          );
      } else if (this.route.snapshot.data.forResource) {
        this.isLoading = true;

        this.service
          .getResourceConfig(
            this.route.parent.snapshot.params.cluster_name,
            this.route.parent.snapshot.params.resource_name
          )
          .subscribe(
            config => this.obj = config,
            error => this.handleError(error),
            () => this.isLoading = false
          );
      } else {
        this.route.parent.data
          .subscribe(data => {
            this.clusterName = data.cluster.name;
            this.loadClusterConfig();
          });
      }
    }
  }

  loadClusterConfig() {
    this.isLoading = true;
    this.service
      .getClusterConfig(this.clusterName)
      .subscribe(
        config => this.obj = config,
        error => this.handleError(error),
        () => this.isLoading = false
      );
  }

  createConfig(value: any) {
    if (this.clusterName) {
      this.isLoading = true;
      this.service
        .setClusterConfig(this.clusterName, value)
        .subscribe(
          () => {
            this.snackBar.open('Configuration added!', 'OK', {
              duration: 2000,
            });
            this.loadClusterConfig();
          },
          error => this.handleError(error),
          () => this.isLoading = false
        );
    }
  }

  updateConfig(value: any) {
    if (this.clusterName) {
      this.isLoading = true;
      this.service
        .setClusterConfig(this.clusterName, value)
        .subscribe(
          () => {
            this.snackBar.open('Configuration updated!', 'OK', {
              duration: 2000,
            });
            this.loadClusterConfig();
          },
          error => this.handleError(error),
          () => this.isLoading = false
        );
    }
  }

  deleteConfig(value: any) {
    if (this.clusterName) {
      this.isLoading = true;
      this.service
        .deleteClusterConfig(this.clusterName, value)
        .subscribe(
          () => {
            this.snackBar.open('Configuration deleted!', 'OK', {
              duration: 2000,
            });
            this.loadClusterConfig();
          },
          error => this.handleError(error),
          () => this.isLoading = false
        );
    }
  }

  protected handleError(error) {
    // the API says if there's no config just return 404 ! sucks!
    this.isLoading = false;
  }
}
