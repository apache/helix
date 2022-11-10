import { map } from 'rxjs/operators';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';

import { Cluster } from '../shared/cluster.model';
import { HelperService } from '../../shared/helper.service';
import { ClusterService } from '../shared/cluster.service';
import { InstanceService } from '../../instance/shared/instance.service';
import { AlertDialogComponent } from '../../shared/dialog/alert-dialog/alert-dialog.component';
import { InputDialogComponent } from '../../shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-cluster-detail',
  templateUrl: './cluster-detail.component.html',
  styleUrls: ['./cluster-detail.component.scss'],
  providers: [InstanceService],
})
export class ClusterDetailComponent implements OnInit {
  readonly tabLinks = [
    { label: 'Dashboard (beta)', link: 'dashboard' },
    { label: 'Resources', link: 'resources' },
    { label: 'Workflows', link: 'workflows' },
    { label: 'Instances', link: 'instances' },
    { label: 'Configuration', link: 'configs' },
  ];

  isLoading = false;
  clusterName: string; // for better UX needs
  cluster: Cluster;
  can = false;

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected dialog: MatDialog,
    protected helperService: HelperService,
    protected clusterService: ClusterService,
    protected instanceService: InstanceService
  ) {}

  ngOnInit() {
    this.clusterService.can().subscribe((data) => (this.can = data));
    this.route.params.pipe(map((p) => p.name)).subscribe((name) => {
      this.clusterName = name;
      this.loadCluster();
    });
  }

  protected loadCluster() {
    this.isLoading = true;
    this.clusterService.get(this.clusterName).subscribe(
      (data) => (this.cluster = data),
      (error) => this.helperService.showError(error),
      () => (this.isLoading = false)
    );
  }

  addInstance() {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: 'Add a new Instance',
          message: 'Please enter the following information to continue:',
          values: {
            host: {
              label: 'Hostname',
            },
            port: {
              label: 'Port',
            },
            enabled: {
              label: 'Enabled',
              type: 'boolean',
            },
          },
        },
      })
      .afterClosed()
      .subscribe((result) => {
        if (result) {
          this.instanceService
            .create(
              this.cluster.name,
              result.host.value,
              result.port.value,
              result.enabled.value
            )
            .subscribe(
              (data) => {
                this.helperService.showSnackBar('New Instance added!');
                // temporarily navigate back to instance view to refresh
                // will fix this using ngrx/store
                this.router.navigate(['workflows'], { relativeTo: this.route });
                setTimeout(() => {
                  this.router.navigate(['instances'], {
                    relativeTo: this.route,
                  });
                }, 100);
              },
              (error) => this.helperService.showError(error),
              () => {}
            );
        }
      });
  }

  enableCluster() {
    this.clusterService.enable(this.clusterName).subscribe(
      () => this.loadCluster(),
      (error) => this.helperService.showError(error)
    );
  }

  disableCluster() {
    this.clusterService.disable(this.clusterName).subscribe(
      () => this.loadCluster(),
      (error) => this.helperService.showError(error)
    );
  }

  activateCluster() {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: 'Activate this Cluster',
          message:
            'To link this cluster to a Helix super cluster (controller), please enter the super cluster name:',
          values: {
            name: {
              label: 'super cluster name',
            },
          },
        },
      })
      .afterClosed()
      .subscribe((result) => {
        if (result && result.name.value) {
          this.clusterService
            .activate(this.clusterName, result.name.value)
            .subscribe(
              () => {
                // since this action may delay a little bit, to prevent re-activation,
                // use an alert dialog to reload the data later
                this.dialog
                  .open(AlertDialogComponent, {
                    data: {
                      title: 'Cluster Activated',
                      message: `Cluster '${this.clusterName}' is linked to super cluster '${result.name.value}'.`,
                    },
                  })
                  .afterClosed()
                  .subscribe(() => {
                    this.loadCluster();
                  });
              },
              (error) => this.helperService.showError(error)
            );
        }
      });
  }

  enableMaintenanceMode() {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: 'Enable maintenance mode',
          message:
            'What reason do you want to use to put the cluster in maintenance mode?',
          values: {
            reason: {
              label: 'reason',
            },
          },
        },
      })
      .afterClosed()
      .subscribe((result) => {
        if (result && result.reason.value) {
          this.clusterService
            .enableMaintenanceMode(this.clusterName, result.reason.value)
            .subscribe(
              () => this.loadCluster(),
              (error) => this.helperService.showError(error)
            );
        }
      });
  }

  disableMaintenanceMode() {
    this.clusterService.disableMaintenanceMode(this.clusterName).subscribe(
      () => this.loadCluster(),
      (error) => this.helperService.showError(error)
    );
  }

  deleteCluster() {
    this.helperService
      .showConfirmation(
        'Are you sure you want to delete this cluster? This cannot be undone.',
        'Confirm Cluster Deletion',
        `Delete Cluster ${this.clusterName}`
      )
      .then((result) => {
        if (result) {
          this.clusterService.remove(this.cluster.name).subscribe((data) => {
            this.helperService.showSnackBar(
              `Cluster ${this.clusterName} deleted`
            );
            // FIXME: should reload cluster list as well
            this.router.navigate(['..'], { relativeTo: this.route });
          });
        }
      });
  }
}
