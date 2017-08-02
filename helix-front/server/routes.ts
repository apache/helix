import * as express from 'express';

import { HelixCtrl } from './controllers/helix';

export default function setRoutes(app) {

  const router = express.Router();

  const helixCtrl = new HelixCtrl(router);

  // Apply the routes to our application with the prefix /api
  app.use('/api', router);

  /* GET /admin to check app health. */
  app.get('/admin', (req, res, next) => {
      res.status(200).send("GOOD");
  });
}
