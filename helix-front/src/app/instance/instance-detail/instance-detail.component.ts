import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Instance } from '../shared/instance.model';
import { InstanceService } from '../shared/instance.service';

@Component({
  selector: 'hi-instance-detail',
  templateUrl: './instance-detail.component.html',
  styleUrls: ['./instance-detail.component.scss'],
  providers: [InstanceService]
})
export class InstanceDetailComponent implements OnInit {

  readonly tabLinks = [
    { label: 'Resources', link: 'resources' },
    { label: 'Configuration', link: 'configs' },
    { label: 'History', link: 'history' }
  ];

  clusterName: string;
  instance: Instance;
  isLoading = true;

  constructor(
    private route: ActivatedRoute,
    private service: InstanceService
  ) { }

  ngOnInit() {
    this.clusterName = this.route.snapshot.params['cluster_name'];
    this.service
      .get(this.clusterName, this.route.snapshot.params['instance_name'])
      .subscribe(
        instance => this.instance = instance,
        error => {},
        () => this.isLoading = false
      );
  }

}
