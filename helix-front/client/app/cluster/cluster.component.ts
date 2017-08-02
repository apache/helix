import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { MediaChange, ObservableMedia } from '@angular/flex-layout';

@Component({
  selector: 'hi-cluster',
  templateUrl: './cluster.component.html',
  styleUrls: ['./cluster.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class ClusterComponent implements OnInit {

  isNarrowView:boolean;

  constructor(
    protected media: ObservableMedia
  ) { }

  ngOnInit() {
    // auto adjust side nav only if not embed
    this.isNarrowView = /*this.headerEnabled &&*/ (this.media.isActive('xs') || this.media.isActive('sm'));
    this.media.subscribe((change: MediaChange) => {
      this.isNarrowView = /*this.headerEnabled &&*/ (change.mqAlias === 'xs' || change.mqAlias === 'sm');
    });
  }

}
