import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { MdDialog } from '@angular/material';
import { MediaChange, ObservableMedia } from '@angular/flex-layout';

import { environment } from '../environments/environment';
import { InputDialogComponent } from './shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {

  headerEnabled = true;
  footerEnabled = environment.production;
  isNarrowView:boolean;

  constructor(
    public dialog: MdDialog,
    protected media: ObservableMedia,
    protected route: ActivatedRoute
  ) {}

  ngOnInit() {

    this.route.queryParams.subscribe(params => {
      if (params['embed'] == 'true') {
        this.headerEnabled = this.footerEnabled = false;
      }
    });

    // auto adjust side nav only if not embed
    this.isNarrowView = this.headerEnabled && (this.media.isActive('xs') || this.media.isActive('sm'));
    this.media.subscribe((change: MediaChange) => {
      this.isNarrowView = this.headerEnabled && (change.mqAlias === 'xs' || change.mqAlias === 'sm');
    });
  }

  openDialog() {
    let ref = this.dialog.open(InputDialogComponent);
    ref.afterClosed().subscribe(result => {
      console.log(result);
    });
  }
}
