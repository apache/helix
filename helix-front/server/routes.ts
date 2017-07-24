import * as express from 'express';

import TestCtrl from './controllers/helix';

export default function setRoutes(app) {

  const router = express.Router();

  const testCtrl = new TestCtrl();

  router.route('/helix/*').get(testCtrl.proxy);
  // other routes
  // router.route('/path1').post(testCtrl.method1);
  // router.route('/path2').put(testCtrl.method2);

  // Apply the routes to our application with the prefix /api
  app.use('/api', router);

}
