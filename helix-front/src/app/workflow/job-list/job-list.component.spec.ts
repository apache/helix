import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { NO_ERRORS_SCHEMA } from '@angular/core';

import { TestingModule } from '../../../testing/testing.module';
import { JobListComponent } from './job-list.component';

describe('JobListComponent', () => {
  let component: JobListComponent;
  let fixture: ComponentFixture<JobListComponent>;

  beforeEach(() => {


    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [JobListComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(JobListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
