import { Component, OnInit } from '@angular/core';
import { MdDialog, MdSnackBar } from '@angular/material';

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
  count = 0;

  constructor(
    protected clusterService: ClusterService,
    protected dialog: MdDialog,
    protected snackBar: MdSnackBar
  ) { }

  ngOnInit() {
    this.loadClusters();
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

  increase() {
    ++this.count;
  }

  createCluster() {
    const dialogRef = this.dialog.open(InputDialogComponent, {
      data: {
        title: 'Create a cluster',
        message: 'Please choose a cluster name:'
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result) {
        this.isLoading = true;
        this.clusterService.create(result)
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
