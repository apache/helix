import { Injectable } from '@angular/core';
import { Headers, Http, Response } from '@angular/http';
import { Router } from '@angular/router';
import { Observable } from 'rxjs/Rx';

import { Settings } from './settings';

@Injectable()
export class HelixService {

  constructor(
    protected router: Router,
    private http: Http
  ) { }

  protected request(path: string, helix?: string): Observable<any> {

    if (helix == null) {
      // fetch helix key from url
      helix = `/${this.router.url.split('/')[1]}`;
    }

console.log('Helix Key: ' + helix);
console.log(this.router.url);

    return this.http
      .get(
        `${Settings.helixAPI}${helix}${path}`,
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
    let message = error.message || 'Cannot reach Helix restful service.';
    console.error(error);
    return Observable.throw(message);
  }
}
