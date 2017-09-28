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
import { UserService } from './core/user.service';

@Component({
  selector: 'hi-root',
  templateUrl: './app.component.html',
  styleUrls: [ './app.component.scss' ],
  providers: [ UserService ]
})
export class AppComponent implements OnInit {

  headerEnabled = true;
  footerEnabled = true;
  isLoading = true;
  currentUser: any;

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected angulartics: Angulartics2Piwik,
    protected service: UserService
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
    this.currentUser = this.service.getCurrentUser();

    this.route.queryParams.subscribe(params => {
      if (params['embed'] == 'true') {
        this.headerEnabled = this.footerEnabled = false;
      }
    });
  }
}
