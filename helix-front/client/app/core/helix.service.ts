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
      helix = this.getHelixKey();
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

  protected post(path: string, data: string): Observable<any> {
    return this.http
      .post(
        `${Settings.helixAPI}${this.getHelixKey()}${path}`,
        data,
        { headers: this.getHeaders() }
      )
      .map(response => response.text().trim() ? response.json() : '{}')
      .catch(this.errorHandler);
  }

  protected put(path: string): Observable<any> {
    return this.http
      .put(
        `${Settings.helixAPI}${this.getHelixKey()}${path}`,
        null,
        { headers: this.getHeaders() }
      )
      .map(response => response.text().trim() ? response.json() : '{}')
      .catch(this.errorHandler);
  }

  protected getHelixKey(): string {
    // fetch helix key from url
    return `/${this.router.url.split('/')[1]}`;
  }

  protected getHeaders() {
    let headers = new Headers();
    headers.append('Accept', 'application/json');
    headers.append('Content-Type', 'application/json');
    return headers;
  }

  protected errorHandler(error: any) {
    let message = error.message || 'Cannot reach Helix restful service.';
    console.error(error);
    return Observable.throw(message);
  }
}
