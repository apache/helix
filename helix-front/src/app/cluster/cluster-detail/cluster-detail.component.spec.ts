import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { beforeEach, describe, expect, xit } from '@jest/globals';

import { TestingModule } from '../../../testing/testing.module';
import { ClusterDetailComponent } from './cluster-detail.component';

describe('ClusterDetailComponent', () => {
  let component: ClusterDetailComponent;
  let fixture: ComponentFixture<ClusterDetailComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [ClusterDetailComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ClusterDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  xit('should create', () => {
    expect(component).toBeTruthy();
  });
});
