import { Request, Response } from 'express';

import * as request from 'request';

export default class HelixCtrl {

  static readonly API_PREFIX = 'http://ltx1-app0693.stg.linkedin.com:12926/admin/v2';
  static readonly ROUTE_PREFIX = '/api/helix';

  proxy = (req: Request, res: Response) => {
    const url = HelixCtrl.API_PREFIX + req.originalUrl.replace(HelixCtrl.ROUTE_PREFIX, '');
    request(url).pipe(res);
  };

}
