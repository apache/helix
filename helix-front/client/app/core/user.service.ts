import { Injectable } from '@angular/core';
import { Headers, Http, Response } from '@angular/http';
import { Router } from '@angular/router';
import { Observable } from 'rxjs/Rx';

import { Settings } from './settings';

@Injectable()
export class UserService {

  constructor(
    protected router: Router,
    private http: Http
  ) { }

  public getCurrentUser(): Observable<string> {
    return this.http
      .get(`${ Settings.userAPI }/current`, { headers: this.getHeaders() })
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
      .map(response => response.json());
  }

  protected getHeaders() {
    let headers = new Headers();
    headers.append('Accept', 'application/json');
    headers.append('Content-Type', 'application/json');
    return headers;
  }
}
