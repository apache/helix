import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Partition, IReplica, Resource } from '../shared/resource.model';

@Component({
  selector: 'hi-partition-list',
  templateUrl: './partition-list.component.html',
  styleUrls: ['./partition-list.component.scss']
})
export class PartitionListComponent implements OnInit {

  @ViewChild('partitionsTable')
  table: any;

  resource: Resource;
  partitions: Partition[];
  rowHeight = 40;
  sorts = [
    { prop: 'isReady', dir: 'asc'},
    { prop: 'name', dir: 'asc'}
  ];

  constructor(protected route: ActivatedRoute) { }

  ngOnInit() {
    if (this.route.parent) {
      this.resource = this.route.parent.snapshot.data.resource;
      this.partitions = this.resource.partitions;
    }
  }

  // check whether we are capable of analysing the states :(
  canAnalyse() {
    return this.partitions// && false
      && this.partitions.length
      && this.resource.replicaCount == this.partitions[0].replicas.length;
  }

  onSelect({ selected }) {
    let row = selected[0];

    this.table.rowDetail.toggleExpandRow(row);
  }
}
