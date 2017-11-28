import * as bodyParser from 'body-parser';
import * as dotenv from 'dotenv';
import * as express from 'express';
import * as morgan from 'morgan';
import * as path from 'path';
import * as fs from 'fs';
import * as http from 'http';
import * as https from 'https';
import * as session from 'express-session';

import { SSL, SESSION_STORE } from './config';
import setRoutes from './routes';

const app = express();
const server = http.createServer(app);

dotenv.load({ path: '.env' });
app.set('port', (process.env.PORT || 3000));

app.use('/', express.static(path.join(__dirname, '../public')));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(session({
  store: SESSION_STORE,
  secret: 'helix',
  resave: true,
  saveUninitialized: true,
  cookie: { expires: new Date(2147483647000) }
}));

app.use(morgan('dev'));

setRoutes(app);

app.get('/*', function(req, res) {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

server.listen(app.get('port'), () => {
  console.log(`App is listening on port ${ app.get('port') } as HTTP`);
});

process.on('uncaughtException', function(err){
  console.error('uncaughtException: ' + err.message);
  console.error(err.stack);
});

// setup SSL
if (SSL.port > 0 && fs.existsSync(SSL.keyfile) && fs.existsSync(SSL.certfile)) {
  let credentials: any = {
    key: fs.readFileSync(SSL.keyfile, 'ascii'),
    cert: fs.readFileSync(SSL.certfile, 'ascii'),
    ca: []
  };

  if (fs.existsSync(SSL.passfile)) {
    credentials.passphrase = fs.readFileSync(SSL.passfile, 'ascii').trim();
  }

  if (SSL.cafiles) {
    SSL.cafiles.forEach(cafile => {
      if (fs.existsSync(cafile)) {
        credentials.ca.push(fs.readFileSync(cafile, 'ascii'));
      }
    });
  }

  const httpsServer = https.createServer(credentials, app);
  httpsServer.listen(SSL.port, () => {
    console.log(`App is listening on port ${ SSL.port } as HTTPS`);
  });
}

export { app };
