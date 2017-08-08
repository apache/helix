import { Request, Response, Router } from 'express';

import * as request from 'request';

import { HELIX_ENDPOINTS } from '../config';

export class HelixCtrl {

  static readonly ROUTE_PREFIX = '/api/helix';

  constructor(router: Router) {
    router.route('/helix/list').get(this.list);
    router.route('/helix/*').all(this.proxy);
  }

  protected proxy(req: Request, res: Response) {
    const url = req.originalUrl.replace(HelixCtrl.ROUTE_PREFIX, '');
    const helixKey = url.split('/')[1];

    const segments = helixKey.split('.');
    const group = segments[0];

    segments.shift();
    const name = segments.join('.');

    const apiPrefix = HELIX_ENDPOINTS[group][name];
    const realUrl = apiPrefix + url.replace(`/${ helixKey }`, '');

    request[req.method.toLowerCase()]({
      url: realUrl,
      json: req.body
    }).pipe(res);

    process.on('uncaughtException', function(err){
      console.error('uncaughtException: ' + err.message);
      console.error(err.stack);
    });
  }

  protected list(req: Request, res: Response) {
    res.json(HELIX_ENDPOINTS);
  }
}
