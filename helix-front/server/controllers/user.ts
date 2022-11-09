import { Response, Router } from 'express';
import * as LdapClient from 'ldapjs';
import * as request from 'request';
import { readFileSync } from 'fs';

import {
  LDAP,
  IDENTITY_TOKEN_SOURCE,
  CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
  SSL,
} from '../config';
import { HelixUserRequest, IdentityTokenPostOptions } from './d';

export class UserCtrl {
  constructor(router: Router) {
    router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
  }

  protected authorize(req: HelixUserRequest, res: Response) {
    //
    // you can rewrite this function
    // to support your own authorization logic
    // by default, do nothing but redirect
    //
    if (req.query.url) {
      res.redirect(req.query.url as string);
    } else {
      res.redirect('/');
    }
  }

  protected current(req: HelixUserRequest, res: Response) {
    res.json(req.session.username || 'Sign In');
  }

  //
  // Check the server-side session store,
  // see if this helix-front ExpressJS server
  // already knows that the current user is an admin.
  //
  protected can(req: HelixUserRequest, res: Response) {
    try {
      return res.json(req.session.isAdmin ? true : false);
    } catch (err) {
      throw new Error(
        `Error from /can logged in admin user session status endpoint: ${err}`
      );
      return false;
    }
  }

  protected login(req: HelixUserRequest, res: Response) {
    const credential = req.body;
    if (!credential.username || !credential.password) {
      res.status(401).json(false);
      return;
    }

    // check LDAP
    const ldap = LdapClient.createClient({ url: LDAP.uri });
    ldap.bind(
      credential.username + LDAP.principalSuffix,
      credential.password,
      (err) => {
        if (err) {
          res.status(401).json(false);
        } else {
          // LDAP login success
          const opts = {
            filter:
              '(&(sAMAccountName=' +
              credential.username +
              ')(objectcategory=person))',
            scope: 'sub',
          };

          req.session.username = credential.username;
          res.set('Username', credential.username);

          ldap.search(LDAP.base, opts, function (err, result) {
            let isInAdminGroup = false;
            result.on('searchEntry', function (entry) {
              if (entry.object && !err) {
                const groups = entry.object['memberOf'];
                for (const group of groups) {
                  const groupName = group.split(',', 1)[0].split('=')[1];
                  if (groupName == LDAP.adminGroup) {
                    isInAdminGroup = true;

                    //
                    // Get an Identity-Token
                    // if an IDENTITY_TOKEN_SOURCE
                    // is specified in the config
                    //
                    if (IDENTITY_TOKEN_SOURCE) {
                      const body = JSON.stringify({
                        username: credential.username,
                        password: credential.password,
                        ...CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
                      });

                      const options: IdentityTokenPostOptions = {
                        url: IDENTITY_TOKEN_SOURCE,
                        json: '',
                        body,
                        headers: {
                          'Content-Type': 'application/json',
                        },
                        agentOptions: {
                          rejectUnauthorized: false,
                        },
                      };

                      if (SSL.cafiles.length > 0) {
                        options.agentOptions.ca = readFileSync(SSL.cafiles[0], {
                          encoding: 'utf-8',
                        });

                        console.log(
                          `ca file from server/controllers/user: ${options.agentOptions.ca}`
                        );
                      }

                      function callback(error, _res, body) {
                        if (error) {
                          throw new Error(
                            `Failed to get ${IDENTITY_TOKEN_SOURCE} Token: ${error}`
                          );
                        } else if (body?.error) {
                          throw new Error(body?.error);
                        } else {
                          const parsedBody = JSON.parse(body);
                          req.session.isAdmin = isInAdminGroup;
                          req.session.identityToken = parsedBody;
                          //
                          // TODO possibly also send identity token
                          // TODO parsedBody to the client as a cookie
                          // TODO Github issue #2236
                          //
                          res.set('Identity-Token-Payload', body);
                          res.json(isInAdminGroup);

                          return parsedBody;
                        }
                      }
                      request.post(options, callback);
                    } else {
                      req.session.isAdmin = isInAdminGroup;
                      res.json(isInAdminGroup);
                    }
                    //
                    // END Get an Identity-Token
                    //
                  }
                }
              } else {
                req.session.isAdmin = isInAdminGroup;
                res.json(isInAdminGroup);
              }
            });
          });
        }
      }
    );
  }
}
