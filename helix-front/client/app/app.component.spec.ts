import { TestBed, async } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';
import { RouterTestingModule } from '@angular/router/testing';

import { Angulartics2, Angulartics2Piwik } from 'angulartics2';

import { AppComponent } from './app.component';

describe('AppComponent', () => {
  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        MaterialModule,
        FlexLayoutModule,
        RouterTestingModule
      ],
      declarations: [
        AppComponent
      ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ],
      // TODO vxu: I don't want to add the following two but ...
      providers: [
        Angulartics2,
        Angulartics2Piwik
      ]
    }).compileComponents();
  }));

  it('should create the app', async(() => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app).toBeTruthy();
  }));

  it(`should have a variable controlling footer`, async(() => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app.footerEnabled).toBeDefined();
  }));

  it('should render title in a md-toolbar', async(() => {
    const fixture = TestBed.createComponent(AppComponent);
    fixture.detectChanges();
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('md-toolbar').textContent).toContain('Helix');
  }));
});
