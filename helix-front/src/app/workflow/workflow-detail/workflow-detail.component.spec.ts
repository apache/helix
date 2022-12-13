import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { TestingModule } from '../../../testing/testing.module';
import { WorkflowDetailComponent } from './workflow-detail.component';
import { WorkflowService } from '../shared/workflow.service';

describe('WorkflowDetailComponent', () => {
  let component: WorkflowDetailComponent;
  let fixture: ComponentFixture<WorkflowDetailComponent>;

  beforeEach(() => {



    TestBed.configureTestingModule({
      imports: [TestingModule],
      providers: [WorkflowService],
      declarations: [WorkflowDetailComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkflowDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
