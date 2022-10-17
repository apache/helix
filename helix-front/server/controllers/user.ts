import { Response, Router } from 'express';
import * as LdapClient from 'ldapjs';
import * as req from 'req';

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
          // login success
          const opts = {
            filter:
              '(&(sAMAccountName=' +
              credential.username +
              ')(objectcategory=person))',
            scope: 'sub',
          };

          ldap.search(LDAP.base, opts, function (err, result) {
            let isInAdminGroup = false;
            result.on('searchEntry', function (entry) {
              if (entry.object && !err) {
                const groups = entry.object['memberOf'];
                for (const group of groups) {
                  const groupName = group.split(',', 1)[0].split('=')[1];
                  if (groupName == LDAP.adminGroup) {
                    isInAdminGroup = true;
                    break;
                  }
                }
              }

              req.session.username = credential.username;
              req.session.isAdmin = isInAdminGroup;
              res.json(isInAdminGroup);
            });
          });
        }
      }
    );
  }
}
