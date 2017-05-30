import { Component, OnInit } from '@angular/core';
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

  footerEnabled = environment.production;
  isNarrowView:boolean;

  constructor(
    public dialog: MdDialog,
    protected media: ObservableMedia
  ) {}

  ngOnInit() {
    // auto adjust side nav
    this.isNarrowView = (this.media.isActive('xs') || this.media.isActive('sm'));
    this.media.subscribe((change: MediaChange) => {
      this.isNarrowView = (change.mqAlias === 'xs' || change.mqAlias === 'sm');
    });
  }

  openDialog() {
    let ref = this.dialog.open(InputDialogComponent);
    ref.afterClosed().subscribe(result => {
      console.log(result);
    });
  }
}
