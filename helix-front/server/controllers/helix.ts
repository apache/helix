import { Request, Response, Router } from 'express';
import * as request from 'request';
import { readFileSync } from 'fs';

import { HELIX_ENDPOINTS, IDENTITY_TOKEN_SOURCE, SSL } from '../config';
import { HelixRequest, HelixRequestOptions } from './d';

export class HelixCtrl {
  static readonly ROUTE_PREFIX = '/api/helix';

  constructor(router: Router) {
    router.route('/helix/list').get(this.list);
    router.route('/helix/*').all(this.proxy);
  }

  protected proxy(req: HelixRequest, res: Response) {
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
      console.log(`helix-rest request url ${realUrl}`);

      const options: HelixRequestOptions = {
        url: realUrl,
        json: req.body,
        headers: {
          'Helix-User': user,
        },
        agentOptions: {
          rejectUnauthorized: false,
        },
      };

      if (SSL.cafiles.length > 0) {
        options.agentOptions.ca = readFileSync(SSL.cafiles[0], {
          encoding: 'utf-8',
        });
      }

      if (IDENTITY_TOKEN_SOURCE) {
        options.headers['Identity-Token'] =
          res.locals.cookie['helixui_identity.token'];
      }

      request[method](options, (error, response, body) => {
        if (error) {
          res.status(response?.statusCode || 500).send(error);
        } else if (body?.error) {
          res.status(response?.statusCode || 500).send(body?.error);
        } else {
          res.status(response?.statusCode).send(body);
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
