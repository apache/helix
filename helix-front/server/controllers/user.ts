import { Request, Response, Router } from 'express';

import * as request from 'request';

import { IsAdmin } from '../config';

export class UserCtrl {

  constructor(router: Router) {
    router.route('/user/authorize').get(this.authorize);
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
  }

  // please rewrite this function to support your own authorization logic
  protected authorize(req: Request, res: Response) {
    if (req.query.name) {
      req.session.username = req.query.name;
      if (req.query.url) {
        res.redirect(req.query.url);
      } else {
        res.redirect('/');
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  }

  protected current(req: Request, res: Response) {
    res.json(req.session.username || 'Guest');
  }

  protected can(req: Request, res: Response) {
    res.json(IsAdmin(req.session.username));
  }
}
