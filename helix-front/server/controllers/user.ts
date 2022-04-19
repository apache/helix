import {Request, Response, Router} from 'express';
import * as LdapClient from 'ldapjs';

import {LDAP} from '../config';

import { HelixRequest } from './d';
export class UserCtrl {

  constructor(router: Router) {
    router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    //   No overload matches this call.
    // The last overload gave the following error.
    //   Argument of type '(req: HelixRequest, res: Response<any, Record<string, any>>) =>
    //   void' is not assignable to parameter of type
    //   'RequestHandlerParams<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.
    // @ts-expect-error
    router.route('/user/current').get(this.current);
    // @ts-expect-error
    router.route('/user/can').get(this.can);
  }

  protected authorize(req: Request, res: Response) {

    // you can rewrite this function to support your own authorization logic
    // by default, doing nothing but redirection

    if (req.query.url) {
      res.redirect(req.query.url as string);
    } else {
      res.redirect('/');
    }
  }

  protected current(req: HelixRequest, res: Response) {
    res.json(req.session.username || 'Sign In');
  }

  protected can(req: HelixRequest, res: Response) {
    res.json(req.session.isAdmin ? true : false);
  }

  protected login(request: HelixRequest, response: Response) {
    const credential = request.body;
    // Property 'username' does not exist on type 'ReadableStream<Uint8Array>'.ts(2339)
    // @ts-expect-error
    if (!credential.username || !credential.password) {
      response.status(401).json(false);
      return;
    }

    // check LDAP
    const ldap = LdapClient.createClient({ url: LDAP.uri });
    // Property 'username' does not exist on type 'ReadableStream<Uint8Array>'.ts(2339)
    // @ts-expect-error
    ldap.bind(credential.username + LDAP.principalSuffix, credential.password, err => {
      if (err) {
        response.status(401).json(false);
      } else {
        // login success
        let opts = {
          filter: '(&(sAMAccountName=' + credential.username + ')(objectcategory=person))',
          scope: 'sub'
        };

        ldap.search(LDAP.base, opts, function(err, result) {
          var isInAdminGroup = false;
          result.on('searchEntry', function (entry) {
            if (entry.object && !err) {
              let groups = entry.object["memberOf"];
              for (var group of groups) {
                const groupName = group.split(",", 1)[0].split("=")[1];
                if (groupName == LDAP.adminGroup) {
                  isInAdminGroup = true;
                  break;
                }
              }
            }

            // Property 'username' does not exist on type 'ReadableStream<Uint8Array>'.ts(2339)
            // @ts-expect-error
            request.session.username = credential.username;
            request.session.isAdmin = isInAdminGroup;
            response.json(isInAdminGroup);
          });
        });
      }
    });
  }
}
