import { Injectable } from '@angular/core';
import { HttpHeaders, HttpClient, HttpResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { Observable, throwError } from 'rxjs';
import { map } from 'rxjs/operators'

import { Settings } from './settings';

@Injectable()
export class HelixService {

  constructor(
    protected router: Router,
    private http: HttpClient
  ) { }

  public can(): Observable<boolean> {
    return this.http
      .get(`${ Settings.userAPI }/can`, { headers: this.getHeaders() })
      // Property 'json' does not exist on type 'Object'.ts(2339)
      // @ts-expect-error
      .pipe(map(response => response.json()))
      // Property 'catch' does not exist on type 'Observable<any>'.ts(2339)
      // @ts-expect-error
      .catch(this.errorHandler);
  }

  protected request(path: string, helix?: string): Observable<any> {

    if (helix == null) {
      helix = this.getHelixKey();
    }

    return this.http
      .get(
        `${Settings.helixAPI}${helix}${path}`,
        { headers: this.getHeaders() }
      )
      // Property 'json' does not exist on type 'Object'.ts(2339)
      // @ts-expect-error
      .pipe(map(response => response.json()))
      // Property 'catch' does not exist on type 'Observable<any>'.ts(2339)
      // @ts-expect-error
      .catch(this.errorHandler);
  }

  protected post(path: string, data: string): Observable<any> {
    return this.http
      .post(
        `${Settings.helixAPI}${this.getHelixKey()}${path}`,
        data,
        { headers: this.getHeaders() }
      )
      // Propertes 'json' and 'text' do not exist on type 'Object'.ts(2339)
      // @ts-expect-error
      .pipe(map(response => response.text().trim() ? response.json() : '{}'))
      // Property 'catch' does not exist on type 'Observable<any>'.ts(2339)
      // @ts-expect-error
      .catch(this.errorHandler);
  }

  protected put(path: string, data: string): Observable<any> {
    return this.http
      .put(
        `${Settings.helixAPI}${this.getHelixKey()}${path}`,
        data,
        { headers: this.getHeaders() }
      )
      // Propertes 'json' and 'text' do not exist on type 'Object'.ts(2339)
      // @ts-expect-error
      .pipe(map(response => response.text().trim() ? response.json() : '{}'))
      // Property 'catch' does not exist on type 'Observable<any>'.ts(2339)
      // @ts-expect-error
      .catch(this.errorHandler);
  }

  protected delete(path: string): Observable<any> {
    return this.http
      .delete(
        `${Settings.helixAPI}${this.getHelixKey()}${path}`,
        { headers: this.getHeaders() }
      )
      // Propertes 'json' and 'text' do not exist on type 'Object'.ts(2339)
      // @ts-expect-error
      .pipe(map(response => response.text().trim() ? response.json() : '{}'))
      // Property 'catch' does not exist on type 'Observable<any>'.ts(2339)
      // @ts-expect-error
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
    console.error(error);

    let message = error.message || 'Cannot reach Helix restful service.';

    if (error instanceof Response) {
      if (error.status == 404) {
        // rest api throws 404 directly to app without any wrapper
        message = 'Not Found';
      } else {
        message = error.text();
        try {
          message = JSON.parse(message).error;
        } catch (e) {}
      }
    }

    return throwError(message);
  }
}
