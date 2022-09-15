import { Component, OnInit, ViewChild } from '@angular/core';
import { MediaChange, MediaObserver } from '@angular/flex-layout';

@Component({
  selector: 'hi-cluster',
  templateUrl: './cluster.component.html',
  styleUrls: ['./cluster.component.scss'],
})
export class ClusterComponent implements OnInit {
  @ViewChild('sidenav', { static: true }) sidenav;

  isNarrowView: boolean;

  constructor(protected media: MediaObserver) {}

  ngOnInit() {
    // auto adjust side nav only if not embed
    this.isNarrowView = this.media.isActive('xs') || this.media.isActive('sm');

    this.media.asObservable().subscribe((change: MediaChange[]) => {
      change.forEach((item) => {
        this.isNarrowView = item.mqAlias === 'xs' || item.mqAlias === 'sm';
      });
    });
  }

  toggleSidenav() {
    this.sidenav.opened ? this.sidenav.close() : this.sidenav.open();
  }
}
