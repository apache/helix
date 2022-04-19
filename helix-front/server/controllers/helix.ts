import { Request, Response, Router } from 'express';

import * as request from 'request';

import { HELIX_ENDPOINTS } from '../config';

export class HelixCtrl {

  static readonly routePrefix = '/api/helix';

  constructor(router: Router) {
    router.route('/helix/list').get(this.list);
    router.route('/helix/*').all(this.proxy);
  }

  protected proxy(req: Request, res: Response) {
    const url = req.originalUrl.replace(HelixCtrl.routePrefix, '');
    const helixKey = url.split('/')[1];

    const segments = helixKey.split('.');
    const group = segments[0];

    segments.shift();
    const name = segments.join('.');

    // Property 'session' does not exist on type
    // 'Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.ts(2339)
    // @ts-expect-error
    const user = req.session.username;
    const method = req.method.toLowerCase();
    // Property 'session' does not exist on type
    // 'Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>'.ts(2339)
    // @ts-expect-error
    if (method != 'get' && !req.session.isAdmin) {
      res.status(403).send('Forbidden');
      return;
    }

    let apiPrefix = null;
    if (HELIX_ENDPOINTS[group]) {
      HELIX_ENDPOINTS[group].forEach(section => {
        if (section[name]) {
          apiPrefix = section[name];
        }
      });
    }

    if (apiPrefix) {
      const realUrl = apiPrefix + url.replace(`/${ helixKey }`, '');
      const options = {
        url: realUrl,
        json: req.body,
        headers: {
          // eslint-disable-next-line @typescript-eslint/naming-convention
          'Helix-User': user
        }
      };
      request[method](options, (error, response, body) => {
        if (error) {
          res.status(500).send(error);
        } else {
          res.status(response.statusCode).send(body);
        }
      });
    } else {
      res.status(404).send('Not found');
    }
  }

  protected list(req: Request, res: Response) {
    res.json(HELIX_ENDPOINTS);
  }
}
