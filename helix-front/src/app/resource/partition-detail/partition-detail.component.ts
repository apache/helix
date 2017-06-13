import { Component, OnInit, Input, ViewEncapsulation } from '@angular/core';

import { Partition, IReplica } from '../shared/resource.model';

@Component({
  selector: 'hi-partition-detail',
  templateUrl: './partition-detail.component.html',
  styleUrls: ['./partition-detail.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class PartitionDetailComponent implements OnInit {

  @Input() clusterName: string;
  @Input() partition: Partition;

  rowHeight = 40;

  constructor() { }

  ngOnInit() {
  }

}
