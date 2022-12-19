import { Component, OnInit } from '@angular/core';
import {
  Router,
  ActivatedRoute,
  NavigationStart,
  NavigationEnd,
  NavigationCancel,
  NavigationError,
} from '@angular/router';
import { MatDialog } from '@angular/material/dialog';

// import { Angulartics2Piwik } from 'angulartics2/piwik';

import { UserService } from './core/user.service';
import { InputDialogComponent } from './shared/dialog/input-dialog/input-dialog.component';
import { HelperService } from './shared/helper.service';

@Component({
  selector: 'hi-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
  providers: [UserService /*, Angulartics2Piwik */],
})
export class AppComponent implements OnInit {
  headerEnabled = true;
  footerEnabled = true;
  isLoading = true;
  currentUser: any;

  constructor(
    // protected angulartics2Piwik: Angulartics2Piwik,
    protected route: ActivatedRoute,
    protected router: Router,
    protected dialog: MatDialog,
    protected service: UserService,
    protected helper: HelperService
  ) {
    router.events.subscribe((event) => {
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
    // angulartics2Piwik.startTracking();
  }

  ngOnInit() {
    this.currentUser = this.service.getCurrentUser();

    this.route.queryParams.subscribe((params) => {
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
              label: 'Username',
            },
            password: {
              label: 'Password',
              type: 'password',
            },
          },
        },
      })
      .afterClosed()
      .subscribe(
        (result) => {
          if (result && result.username.value && result.password.value) {
            this.service
              .login(result.username.value, result.password.value)
              .subscribe(
                (loginResponse) => {
                  if (!loginResponse) {
                    this.helper.showError(
                      `${loginResponse.status}: Either You are not part of helix-admin LDAP group or your password is incorrect.`
                    );
                  }

                  this.currentUser = this.service.getCurrentUser();
                },
                (error) => {
                  // since rest API simply throws 404 instead of empty config when config is not initialized yet
                  // frontend has to treat 404 as normal result
                  if (error != 'Not Found') {
                    this.helper.showError(error);
                  }
                  this.isLoading = false;
                }
              );
          }
        },
        (error) => {
          // since rest API simply throws 404 instead of empty config when config is not initialized yet
          // frontend has to treat 404 as normal result
          if (error != 'Not Found') {
            this.helper.showError(error);
          }
          this.isLoading = false;
        }
      );
  }
}
