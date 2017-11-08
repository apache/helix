import { Component, Input } from '@angular/core';

@Component({
  selector: 'hi-detail-header',
  templateUrl: './detail-header.component.html',
  styleUrls: ['./detail-header.component.scss']
})
export class DetailHeaderComponent {

  @Input() cluster;
  @Input() resource;
  @Input() instance;
  @Input() controller;
  @Input() workflow;

  constructor() { }

  isSecondary() {
    return this.controller || this.instance || this.resource || this.workflow;
  }

  getTag() {
    if (this.controller) {
      return 'controller';
    }
    if (this.instance) {
      return 'instance';
    }
    if (this.resource) {
      return 'resource';
    }
    if (this.workflow) {
      return 'workflow';
    }
    return 'cluster';
  }

}
