import { Component, OnInit, ViewChild } from '@angular/core';
import { MediaChange, ObservableMedia } from '@angular/flex-layout';

@Component({
  selector: 'hi-cluster',
  templateUrl: './cluster.component.html',
  styleUrls: ['./cluster.component.scss']
})
export class ClusterComponent implements OnInit {

  @ViewChild('sidenav') sidenav;

  isNarrowView: boolean;

  constructor(
    protected media: ObservableMedia
  ) { }

  ngOnInit() {
    // auto adjust side nav only if not embed
    this.isNarrowView = (this.media.isActive('xs') || this.media.isActive('sm'));

    this.media.subscribe((change: MediaChange) => {
      this.isNarrowView = (change.mqAlias === 'xs' || change.mqAlias === 'sm');
    });
  }

  toggleSidenav() {
    this.sidenav.opened ? this.sidenav.close() : this.sidenav.open();
  }

}
