import { Component, OnInit } from '@angular/core';
import { MatDialog, MatSnackBar } from '@angular/material';

import { ClusterService } from '../shared/cluster.service';
import { Cluster } from '../shared/cluster.model';
import { AlertDialogComponent } from '../../shared/dialog/alert-dialog/alert-dialog.component';
import { InputDialogComponent } from '../../shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-cluster-list',
  templateUrl: './cluster-list.component.html',
  styleUrls: ['./cluster-list.component.scss']
})
export class ClusterListComponent implements OnInit {

  clusters: Cluster[] = [];
  errorMessage: string = '';
  isLoading: boolean = true;
  can: boolean = false;

  constructor(
    protected clusterService: ClusterService,
    protected dialog: MatDialog,
    protected snackBar: MatSnackBar
  ) { }

  ngOnInit() {
    this.loadClusters();
    this.clusterService.can().subscribe(data => this.can = data);
  }

  loadClusters() {
    this.clusterService
      .getAll()
      .subscribe(
        /* happy path */ clusters => this.clusters = clusters,
        /* error path */ error => this.showErrorMessage(error),
        /* onComplete */ () => this.isLoading = false
      );
  }

  showErrorMessage(error: any) {
    this.errorMessage = error;
    this.isLoading = false;
    this.dialog.open(AlertDialogComponent, {
      data: {
        title: 'Error',
        message: this.errorMessage
      }
    });
  }

  createCluster() {
    const dialogRef = this.dialog.open(InputDialogComponent, {
      data: {
        title: 'Create a cluster',
        message: 'Please enter the following information to continue:',
        values: {
          name: {
            label: 'new cluster name'
          }
        }
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result && result.name && result.name.value) {
        this.isLoading = true;
        this.clusterService.create(result.name.value)
          .subscribe(data => {
            this.snackBar.open('Cluster created!', 'OK', {
              duration: 2000,
            });
            this.loadClusters();
          });
      }
    });
  }

}
