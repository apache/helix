import { Request, Response, Router } from "express";
import * as LdapClient from "ldapjs";

import { LDAP } from "../config";
import { HelixUserRequest } from "./d";

export class UserCtrl {
  constructor(router: Router) {
    router.route("/user/authorize").get(this.authorize);
    router.route("/user/login").post(this.login.bind(this));
    router.route("/user/current").get(this.current);
    router.route("/user/can").get(this.can);
  }

  protected authorize(req: HelixUserRequest, res: Response) {
    // you can rewrite this function to support your own authorization logic
    // by default, doing nothing but redirection
    if (req.query.url) {
      res.redirect(req.query.url as string);
    } else {
      res.redirect("/");
    }
  }

  protected current(req: HelixUserRequest, res: Response) {
    res.json(req.session.username || "Sign In");
  }

  protected can(req: HelixUserRequest, res: Response) {
    try {
      return res.json(req.session.isAdmin ? true : false);
    } catch (err) {
      // console.log('error from can', err)
      return false;
    }
  }

  protected login(request: HelixUserRequest, response: Response) {
    const credential = request.body;
    if (!credential.username || !credential.password) {
      response.status(401).json(false);
      return;
    }

    // check LDAP
    const ldap = LdapClient.createClient({ url: LDAP.uri });
    ldap.bind(
      credential.username + LDAP.principalSuffix,
      credential.password,
      (err) => {
        if (err) {
          response.status(401).json(false);
        } else {
          // login success
          const opts = {
            filter:
              "(&(sAMAccountName=" +
              credential.username +
              ")(objectcategory=person))",
            scope: "sub",
          };

          ldap.search(LDAP.base, opts, function (err, result) {
            let isInAdminGroup = false;
            result.on("searchEntry", function (entry) {
              if (entry.object && !err) {
                const groups = entry.object["memberOf"];
                for (const group of groups) {
                  const groupName = group.split(",", 1)[0].split("=")[1];
                  if (groupName == LDAP.adminGroup) {
                    isInAdminGroup = true;
                    break;
                  }
                }
              }

              request.session.username = credential.username;
              request.session.isAdmin = isInAdminGroup;
              response.json(isInAdminGroup);
            });
          });
        }
      }
    );
  }
}
