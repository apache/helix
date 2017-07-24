import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

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

  constructor(
    private route: ActivatedRoute,
    private serivce: ConfigurationService
  ) { }

  ngOnInit() {
    if (this.route.parent) {
      // TODO vxu: convert this logic to config.resolver
      if (this.route.snapshot.data.forInstance) {
        this.isLoading = true;

        this.serivce
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

        this.serivce
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
            this.isLoading = true;

            this.serivce
              .getClusterConfig(data.cluster.name)
              .subscribe(
                config => this.obj = config,
                error => this.handleError(error),
                () => this.isLoading = false
              );
          });
      }
    }
  }

  protected handleError(error) {
    // the API says if there's no config just return 404 ! sucks!
    this.isLoading = false;
  }

}
