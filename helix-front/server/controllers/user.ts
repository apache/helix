import {Request, Response, Router} from 'express';
import * as LdapClient from 'ldapjs';

import {LDAP} from '../config';

export class UserCtrl {

  constructor(router: Router) {
    router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
  }

  protected authorize(req: Request, res: Response) {

    // you can rewrite this function to support your own authorization logic
    // by default, doing nothing but redirection

    if (req.query.url) {
      // Argument of type 'string | ParsedQs | string[] | ParsedQs[]' is not assignable to parameter of type 'string'.
      // Type 'ParsedQs' is not assignable to type 'string'.ts(2345)
      // @ts-expect-error
      res.redirect(req.query.url);
    } else {
      res.redirect('/');
    }
  }

  protected current(req: Request, res: Response) {
    // Property 'session' does not exist on type 'Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.ts(2339)
    // @ts-expect-error
    res.json(req.session.username || 'Sign In');
  }

  protected can(req: Request, res: Response) {
    try {
      // Property 'session' does not exist on type 'Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.ts(2339)
      // @ts-expect-error
      return res.json(req.session.isAdmin ? true : false);
    } catch (err) {
      console.log('error from can', err)
      return false
    }
  }

  protected login(request: Request, response: Response) {
    const credential = request.body;
    if (!credential.username || !credential.password) {
      response.status(401).json(false);
      return;
    }

    // check LDAP
    const ldap = LdapClient.createClient({ url: LDAP.uri });
    ldap.bind(credential.username + LDAP.principalSuffix, credential.password, err => {
      if (err) {
        response.status(401).json(false);
      } else {
        // login success
        const opts = {
          filter: '(&(sAMAccountName=' + credential.username + ')(objectcategory=person))',
          scope: 'sub'
        };

        ldap.search(LDAP.base, opts, function(err, result) {
          let isInAdminGroup = false;
          result.on('searchEntry', function(entry) {
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

            // Property 'session' does not exist on type 'Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.ts(2339)
            // @ts-expect-error
            request.session.username = credential.username;
            // Property 'session' does not exist on type 'Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.ts(2339)
            // @ts-expect-error
            request.session.isAdmin = isInAdminGroup;
            response.json(isInAdminGroup);
          });
        });
      }
    });
  }
}
