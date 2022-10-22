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
import {
  TOKEN_RESPONSE_KEY,
  TOKEN_EXPIRATION_KEY,
  IDENTITY_TOKEN_SOURCE,
} from '../../server/config';

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
                      "You're not part of helix-admin group or password incorrect"
                    );
                  }
                  console.log(
                    'loginResponse from app.component login subscription',
                    loginResponse
                  );

                  //
                  // set cookie with Identity Token
                  // if an Identity Token Source is configured
                  //
                  if (IDENTITY_TOKEN_SOURCE && TOKEN_RESPONSE_KEY) {
                    const identityTokenPayload = loginResponse.headers.get(
                      'Identity-Token-Payload'
                    );
                    console.log(
                      `Identity-Token-Payload from app.component ${identityTokenPayload}`
                    );

                    const parsedIdentityTokenPayload =
                      JSON.parse(identityTokenPayload);
                    console.log(
                      'parsedIdentityTokenPayload',
                      parsedIdentityTokenPayload
                    );

                    const cookie = {
                      name: 'helixui_identity.token',
                      value:
                        parsedIdentityTokenPayload.value[TOKEN_RESPONSE_KEY],
                      expirationDate: new Date(
                        parsedIdentityTokenPayload.value[TOKEN_EXPIRATION_KEY]
                      ).toUTCString(),
                    };

                    const cookieString = `${cookie.name}=${
                      cookie.value || ''
                    }; expires=${cookie.expirationDate}; path=/; domain=`;
                    console.log('cookieString', cookieString);
                    document.cookie = cookieString;
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
