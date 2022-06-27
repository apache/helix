import { Request, Response, Router } from 'express';

import * as request from 'request';

import { HELIX_ENDPOINTS } from '../config';
import { HelixUserRequest } from './d';

export class HelixCtrl {
  static readonly ROUTE_PREFIX = '/api/helix';

  constructor(router: Router) {
    router.route('/helix/list').get(this.list);
    router.route('/helix/*').all(this.proxy);
  }

  protected proxy(req: HelixUserRequest, res: Response) {
    const url = req.originalUrl.replace(HelixCtrl.ROUTE_PREFIX, '');
    const helixKey = url.split('/')[1];

    const segments = helixKey.split('.');
    const group = segments[0];

    segments.shift();
    const name = segments.join('.');

    const user = req.session.username;
    const method = req.method.toLowerCase();
    if (method != 'get' && !req.session.isAdmin) {
      res.status(403).send('Forbidden');
      return;
    }

    let apiPrefix = null;
    if (HELIX_ENDPOINTS[group]) {
      HELIX_ENDPOINTS[group].forEach((section) => {
        if (section[name]) {
          apiPrefix = section[name];
        }
      });
    }

    if (apiPrefix) {
      const realUrl = apiPrefix + url.replace(`/${helixKey}`, '');
      console.log('realUrl', realUrl);
      console.log('request body', JSON.stringify(req.body, null, 2))
      const options = {
        url: realUrl,
        json: req.body,
        headers: {
          'Helix-User': user,
        },
      };
      request[method](options, (error, response, body) => {
        console.log('body from inner request function in server/controllers/helix.ts')
        console.log(JSON.stringify(body, null, 2))
        if (error) {
          res.status(500).send(error);
        } else if(body?.error) {
          res.status(500).send(body?.error);
        } else {
          res.status(response.statusCode).send(body);
        }
      });
    } else {
      res.status(404).send('Not found');
    }
  }

  protected list(req: Request, res: Response) {
    try {
      res.json(HELIX_ENDPOINTS);
    } catch (err) {
      console.log('error from helix/list/');
      console.log(err);
    }
  }
}
