import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { MdDialog } from '@angular/material';

import { Cluster } from '../shared/cluster.model';
import { HelperService } from '../../shared/helper.service';
import { ClusterService } from '../shared/cluster.service';
import { InstanceService } from '../../instance/shared/instance.service';
import { InputDialogComponent } from '../../shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-cluster-detail',
  templateUrl: './cluster-detail.component.html',
  styleUrls: ['./cluster-detail.component.scss'],
  providers: [InstanceService]
})
export class ClusterDetailComponent implements OnInit {

  readonly tabLinks = [
    { label: 'Resources', link: 'resources' },
    { label: 'Workflows', link: 'workflows' },
    { label: 'Instances', link: 'instances' },
    { label: 'Configuration', link: 'configs' }
  ];

  cluster: Cluster;
  can = false;

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected dialog: MdDialog,
    protected helperService: HelperService,
    protected clusterService: ClusterService,
    protected instanceService: InstanceService
  ) {
  }

  ngOnInit() {
    this.route.data.subscribe(data => this.cluster = data.cluster);
    this.clusterService.can().subscribe(data => this.can = data);
    this.route.params
      .map(p => p.name)
      .subscribe(name => {
        console.log(name);
      });
  }

  addInstance() {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: 'Add a new Instance',
          message: 'Please enter the following information to continue:',
          values: {
            host: {
              label: 'Hostname'
            },
            port: {
              label: 'Port'
            },
            enabled: {
              label: 'Enabled',
              type: 'boolean'
            }
          }
        }
      })
      .afterClosed()
      .subscribe(result => {
        if (result) {
          this.instanceService
            .create(this.cluster.name, result.host.value, result.port.value, result.enabled.value)
            .subscribe(
              data => {
                this.helperService.showSnackBar('New Instance added!');
                // temporarily navigate back to instance view to refresh
                // will fix this using ngrx/store
                this.router.navigate(['workflows'], { relativeTo: this.route });
                setTimeout(() => {
                    this.router.navigate(['instances'], { relativeTo: this.route });
                }, 100);
              },
              error => this.helperService.showError(error),
              () => {}
            );
        }
      });
  }

  deleteCluster() {
    // disable delete function right now since it's too dangerous
    /*
    this.dialog
      .open(ConfirmDialogComponent, {
        data: {
          title: 'Confirmation',
          message: 'Are you sure you want to delete this cluster?'
        }
      })
      .afterClosed()
      .subscribe(result => {
        if (result) {
          this.clusterService
            .remove(this.cluster.name)
            .subscribe(data => {
              this.snackBar.open('Cluster deleted!', 'OK', {
                duration: 2000,
              });
              // FIXME: should reload cluster list as well
              this.router.navigate(['..'], { relativeTo: this.route });
            });
        }
      });
      */
  }

}
