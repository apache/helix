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
import { ResourceNodeViewerComponent } from './resource-node-viewer.component';
import { ResourceService } from '../shared/resource.service';

describe('ResourceNodeViewerComponent', () => {
  let component: ResourceNodeViewerComponent;
  let fixture: ComponentFixture<ResourceNodeViewerComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [ResourceNodeViewerComponent],
      providers: [ResourceService],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceNodeViewerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
