import 'zone.js';
import 'zone.js/dist/zone-testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { TestingModule } from '../../../testing/testing.module';
import { ConfigDetailComponent } from './config-detail.component';

describe('ConfigDetailComponent', () => {
  let component: ConfigDetailComponent;
  let fixture: ComponentFixture<ConfigDetailComponent>;

  beforeEach(() => {


    TestBed.configureTestingModule({
      imports: [TestingModule, HttpClientTestingModule],
      declarations: [ConfigDetailComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
