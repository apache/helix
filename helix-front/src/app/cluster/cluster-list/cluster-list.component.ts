import { Component, OnInit } from '@angular/core';

import { ClusterService } from '../shared/cluster.service';
import { Cluster } from '../shared/cluster.model';

@Component({
  selector: 'hi-cluster-list',
  templateUrl: './cluster-list.component.html',
  styleUrls: ['./cluster-list.component.scss'],
  providers: [ClusterService]
})
export class ClusterListComponent implements OnInit {

  clusters: Cluster[] = [];
  errorMessage: string = '';
  isLoading: boolean = true;

  constructor(private clusterService: ClusterService) { }

  ngOnInit() {
    this.clusterService
      .getAll()
      .subscribe(
        /* happy path */ clusters => this.clusters = clusters,
        /* error path */ error => this.errorMessage = error,
        /* onComplete */ () => this.isLoading = false
      );
  }

}
