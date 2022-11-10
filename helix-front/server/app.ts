import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import fs from 'fs';
import http from 'http';
import https from 'https';
import session from 'express-session';

import { SSL, SESSION_STORE } from './config';
import setRoutes from './routes';

const app = express();
const server = http.createServer(app);

dotenv.load({ path: '.env' });
app.set('port', process.env.PORT || 4200);

app.use('/', express.static(path.join(__dirname, '../public')));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(
  session({
    store: SESSION_STORE,
    secret: 'helix',
    resave: true,
    saveUninitialized: true,
    cookie: { expires: new Date(2147483647000) },
  })
);

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
