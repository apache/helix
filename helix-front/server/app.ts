import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import fs from 'fs';
import http from 'http';
import https from 'https';
import session from 'express-session';
import * as appInsights from 'applicationinsights';
import ProxyAgent from 'proxy-agent';

import {
  APP_INSIGHTS_CONNECTION_STRING,
  PROXY_URL,
  SSL,
  SESSION_STORE,
} from './config';
import setRoutes from './routes';

const isProd = process.env.NODE_ENV === 'production';
const httpsProxyAgent = PROXY_URL ? new ProxyAgent(PROXY_URL) : null;

if (APP_INSIGHTS_CONNECTION_STRING) {
  appInsights
    .setup(APP_INSIGHTS_CONNECTION_STRING)
    .setAutoDependencyCorrelation(true)
    .setAutoCollectRequests(true)
    .setAutoCollectPerformance(true, true)
    .setAutoCollectExceptions(true)
    .setAutoCollectDependencies(true)
    .setAutoCollectConsole(true)
    .setUseDiskRetryCaching(true)
    .setSendLiveMetrics(false)
    .setDistributedTracingMode(appInsights.DistributedTracingModes.AI)
    .start();
}

if (httpsProxyAgent && isProd) {
  // NOTES:
  //
  // - `defaultClient` property on `appInsights` doesn't exist
  // until `.start` is called
  //
  // - in development on our laptop (as opposed to a server)
  // we don't need to go through a proxy.
  appInsights.defaultClient.config.httpsAgent = httpsProxyAgent;
}

const app = express();
const server = http.createServer(app);

dotenv.load({ path: '.env' });
app.set('port', process.env.PORT || 4200);

const secretToken = process.env.SECRET_TOKEN;
if (!secretToken || secretToken === 'promiseyouwillchangeit') {
  if (isProd) {
    throw new Error('Please change your SECRET_TOKEN env');
  } else {
    console.warn(
      'Remember to change your SECRET_TOKEN env before deploying to PROD'
    );
  }
}

app.use('/', express.static(path.join(__dirname, '../public')));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(
  session({
    store: SESSION_STORE,
    secret: secretToken,
    resave: true,
    saveUninitialized: true,
    cookie: { expires: new Date(2147483647000) },
  })
);

if (APP_INSIGHTS_CONNECTION_STRING) {
  app.use(function appInsightsMiddleWare(req, res, next) {
    appInsights.defaultClient.trackNodeHttpRequest({
      request: req,
      response: res,
    });
    next();
  });
}

app.use(morgan('dev'));

app.use((req, res, next) => {
  const {
    headers: { cookie },
  } = req;
  if (cookie) {
    const values = cookie.split(';').reduce((res, item) => {
      const data = item.trim().split('=');
      return { ...res, [data[0]]: data[1] };
    }, {});
    res.locals.cookie = values;
  } else res.locals.cookie = {};
  next();
});

setRoutes(app);

app.get('/*', function (_req, res) {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

server.listen(app.get('port'), () => {
  console.log(`App is listening on port ${app.get('port')} as HTTP`);
});

process.on('uncaughtException', function (err) {
  console.error('uncaughtException: ' + err.message);
  console.error(err.stack);
});

// setup SSL
if (SSL.port > 0 && fs.existsSync(SSL.keyfile) && fs.existsSync(SSL.certfile)) {
  const credentials: any = {
    key: fs.readFileSync(SSL.keyfile, 'ascii'),
    cert: fs.readFileSync(SSL.certfile, 'ascii'),
    ca: [],
  };

  if (fs.existsSync(SSL.passfile)) {
    credentials.passphrase = fs.readFileSync(SSL.passfile, 'ascii').trim();
  }

  if (SSL.cafiles) {
    SSL.cafiles.forEach((cafile) => {
      if (fs.existsSync(cafile)) {
        credentials.ca.push(fs.readFileSync(cafile, 'ascii'));
      }
    });
  }

  const httpsServer = https.createServer(credentials, app);
  httpsServer.listen(SSL.port, () => {
    console.log(`App is listening on port ${SSL.port} as HTTPS`);
  });
}

export { app };
