import { Injectable } from '@angular/core';
import { Headers, Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Rx';

import { environment } from '../../environments/environment';

@Injectable()
export class HelixService {

  constructor(private http: Http) { }

  protected request(path: string): Observable<any> {
    return this.http
      .get(
        `${environment.helixAPI}${path}`,
        { headers: this.getHeaders() }
      )
      .map(response => response.json())
      .catch(this.errorHandler);
  }

  protected getHeaders() {
    let headers = new Headers();
    headers.append('Accept', 'application/json');
    return headers;
  }

  protected errorHandler(error: any) {
    let message = error.message || 'The furthest distance in the world\nIs not between life and death\nBut when the page shows in front of you\nYet you don\'t know that it can\'t talk to the backend\n -Vivo';
    console.error(message);
    return Observable.throw(message);
  }
}
