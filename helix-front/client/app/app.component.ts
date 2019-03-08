import { Component, OnInit } from '@angular/core';
import {
  Router,
  ActivatedRoute,
  NavigationStart,
  NavigationEnd,
  NavigationCancel,
  NavigationError
} from '@angular/router';
import { MatDialog } from '@angular/material';

import { Angulartics2Piwik } from 'angulartics2';

import { UserService } from './core/user.service';
import { InputDialogComponent } from './shared/dialog/input-dialog/input-dialog.component';
import { HelperService } from './shared/helper.service';
import {LDAP} from "../../server/config";

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
    protected dialog: MatDialog,
    protected service: UserService,
    protected helper: HelperService
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

  login() {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: 'Sign In',
          message: 'Please enter your LDAP username and password to continue:',
          values: {
            username: {
              label: 'Username'
            },
            password: {
              label: 'Password',
              type: 'password'
            }
          }
        }
      })
      .afterClosed()
      .subscribe(result => {
        if (result && result.username.value && result.password.value) {
          this.service
            .login(result.username.value, result.password.value)
            .subscribe(
              isAuthorized => {
                if (!isAuthorized) {
                  this.helper.showError("You're not part of " + LDAP.adminGroup + " group or password incorrect");
                }
                this.currentUser = this.service.getCurrentUser();
              },
              error => this.helper.showError(error)
            );
        }
      });
  }
}
