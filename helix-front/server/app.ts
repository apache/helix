import * as bodyParser from 'body-parser';
import * as dotenv from 'dotenv';
import * as express from 'express';
import * as morgan from 'morgan';
import * as path from 'path';

import setRoutes from './routes';

const app = express();
dotenv.load({ path: '.env' });
app.set('port', (process.env.PORT || 3000));

app.use('/', express.static(path.join(__dirname, '../public')));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(morgan('dev'));

setRoutes(app);

app.get('/*', function(req, res) {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(app.get('port'), () => {
  console.log('App is listening on port ' + app.get('port'));
});

export { app };
