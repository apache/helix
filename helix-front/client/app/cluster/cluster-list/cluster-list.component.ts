import { Component, OnInit } from '@angular/core';
import { MdDialog } from '@angular/material';

import { ClusterService } from '../shared/cluster.service';
import { Cluster } from '../shared/cluster.model';
import { AlertDialogComponent } from '../../shared/dialog/alert-dialog/alert-dialog.component';

@Component({
  selector: 'hi-cluster-list',
  templateUrl: './cluster-list.component.html',
  styleUrls: ['./cluster-list.component.scss']
})
export class ClusterListComponent implements OnInit {

  clusters: Cluster[] = [];
  errorMessage: string = '';
  isLoading: boolean = true;

  constructor(
    protected clusterService: ClusterService,
    protected dialog: MdDialog
  ) { }

  ngOnInit() {
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

}
