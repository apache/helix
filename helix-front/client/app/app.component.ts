import { Component, OnInit } from '@angular/core';
import {
  Router,
  ActivatedRoute,
  NavigationStart,
  NavigationEnd,
  NavigationCancel,
  NavigationError
} from '@angular/router';

import { Angulartics2Piwik } from 'angulartics2';

import { environment } from '../environments/environment';

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
}
