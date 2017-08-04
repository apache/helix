import { Component, OnInit } from '@angular/core';
import {
  Router,
  ActivatedRoute,
  NavigationStart,
  NavigationEnd,
  NavigationCancel,
  NavigationError
} from '@angular/router';
import { MdDialog } from '@angular/material';

import { Angulartics2Piwik } from 'angulartics2';

import { environment } from '../environments/environment';
import { InputDialogComponent } from './shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {

  headerEnabled = true;
  footerEnabled = true;
  isLoading = true;

  constructor(
    public dialog: MdDialog,
    protected route: ActivatedRoute,
    protected router: Router,
    protected angulartics: Angulartics2Piwik
  ) {
    router.events.subscribe(event => {
      if (event instanceof NavigationStart) {
        this.isLoading = true;
      }
      if (event instanceof NavigationEnd) {
        this.isLoading = false;
      }
      if (event instanceof NavigationError) {
        this.isLoading = false;
      }
      if (event instanceof NavigationCancel) {
        this.isLoading = false;
      }
    });
  }

  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      if (params['embed'] == 'true') {
        this.headerEnabled = this.footerEnabled = false;
      }
    });
  }

  openDialog() {
    let ref = this.dialog.open(InputDialogComponent);
    ref.afterClosed().subscribe(result => {
      console.log(result);
    });
  }
}
