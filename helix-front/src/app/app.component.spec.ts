import 'zone.js';
import 'zone.js/dist/zone-testing';

import { TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import {
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  xit,
} from '@jest/globals';

// import { Angulartics2 } from 'angulartics2';
// import { Angulartics2Piwik } from 'angulartics2/piwik';

import { TestingModule } from '../testing/testing.module';
import { AppComponent } from './app.component';

describe('AppComponent', () => {
  beforeAll(() => {});

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [AppComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
      // TODO vxu: I don't want to add the following two but ...
      providers: [
        // Angulartics2,
        // Angulartics2Piwik,
      ],
    }).compileComponents();
  });

  it('should create the app', () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app).toBeTruthy();
  });

  it(`should have a variable controlling footer`, () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app.footerEnabled).toBeDefined();
  });

  xit('should render title in a mat-toolbar', () => {
    const fixture = TestBed.createComponent(AppComponent);
    fixture.detectChanges();
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('mat-toolbar').textContent).toContain(
      'Helix'
    );
  });
});
