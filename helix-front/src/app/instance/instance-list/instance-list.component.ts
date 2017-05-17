import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'hi-instance-list',
  templateUrl: './instance-list.component.html',
  styleUrls: ['./instance-list.component.scss']
})
export class InstanceListComponent implements OnInit {

  instances: any[];

  constructor(private route: ActivatedRoute) { }

  ngOnInit() {
    if (this.route.parent) {
      this.route.parent.data.subscribe(data => {
        this.instances = data.cluster.instances;
      });
    }
  }

}
