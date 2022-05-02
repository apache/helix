import { Injectable } from '@angular/core';
import { HttpHeaders, HttpClient, HttpResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { Observable } from 'rxjs/Rx';

import { Settings } from './settings';

@Injectable()
export class UserService {

  constructor(
    protected router: Router,
    private http: HttpClient
  ) { }

  public getCurrentUser(): Observable<string> {
    return this.http
      .get(`${ Settings.userAPI }/current`, { headers: this.getHeaders() })
      // @ts-expect-error
      // Property 'json' does not exist on type 'Object'.ts(2339)
      .map(response => response.json())
      .catch(_ => _);
  }

  public login(username: string, password: string): Observable<boolean> {
    return this.http
      .post(
        `${ Settings.userAPI }/login`,
        { username: username, password: password },
        { headers: this.getHeaders() }
      )
      // @ts-expect-error
      // Property 'json' does not exist on type 'Object'.ts(2339)
      .map(response => response.json());
  }

  protected getHeaders() {
    let headers = new HttpHeaders();
    headers.append('Accept', 'application/json');
    headers.append('Content-Type', 'application/json');
    return headers;
  }
}
