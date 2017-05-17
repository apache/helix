import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Controller } from '../shared/controller.model';
import { ControllerService } from '../shared/controller.service';

@Component({
  selector: 'hi-controller-detail',
  templateUrl: './controller-detail.component.html',
  styleUrls: ['./controller-detail.component.scss'],
  providers: [ControllerService]
})
export class ControllerDetailComponent implements OnInit {

  clusterName: string;
  controller: Controller;
  isLoading = true;

  constructor(
    private route: ActivatedRoute,
    private service: ControllerService
  ) { }

  ngOnInit() {
    this.clusterName = this.route.snapshot.params['name'];
    this.service
      .get(this.clusterName)
      .subscribe(
        controller => this.controller = controller,
        error => {},
        () => this.isLoading = false
      );
  }

}
