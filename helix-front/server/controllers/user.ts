import { Response, Router } from 'express';
import * as LdapClient from 'ldapjs';
import * as request from 'request';

import {
  LDAP,
  IDENTITY_TOKEN_SOURCE,
  CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
} from '../config';
import { HelixUserRequest } from './d';

export class UserCtrl {
  constructor(router: Router) {
    router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
  }

  protected authorize(req: HelixUserRequest, res: Response) {
    // you can rewrite this function to support your own authorization logic
    // by default, doing nothing but redirection
    if (req.query.url) {
      res.redirect(req.query.url as string);
    } else {
      res.redirect('/');
    }
  }

  protected current(req: HelixUserRequest, res: Response) {
    res.json(req.session.username || 'Sign In');
  }

  protected can(req: HelixUserRequest, res: Response) {
    try {
      return res.json(req.session.isAdmin ? true : false);
    } catch (err) {
      // console.log('error from can', err)
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

                      const options = {
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

                      function callback(error, _res, body) {
                        if (error) {
                          throw new Error(
                            `Failed to get ${IDENTITY_TOKEN_SOURCE} Token: ${error}`
                          );
                        } else if (body?.error) {
                          throw new Error(body?.error);
                        } else {
                          const parsedBody = JSON.parse(body);
                          console.log(
                            'parsedBody from identity token source call',
                            parsedBody
                          );

                          console.log('3rd outer-most else');
                          req.session.isAdmin = isInAdminGroup;
                          //
                          // TODO possibly also send identity token
                          // TODO parsedBody to the client as a cookie
                          // TODO Github issue #2236
                          //
                          req.session.identityToken = parsedBody;
                          res.set('Identity-Token-Payload', body);
                          res.json(isInAdminGroup);

                          return parsedBody;
                        }
                      }
                      request.post(options, callback);
                    } else {
                      console.log('2nd outer-most else');
                      req.session.isAdmin = isInAdminGroup;
                      res.json(isInAdminGroup);
                    }
                    //
                    // END Get and Identity-Token
                    //
                  }
                }
              } else {
                console.log('outer-most else');
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
