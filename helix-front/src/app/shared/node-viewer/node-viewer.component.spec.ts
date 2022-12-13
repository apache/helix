import 'zone.js';
import 'zone.js/dist/zone-testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { TestingModule } from '../../../testing/testing.module';
import { beforeAll, beforeEach, describe, expect, it } from '@jest/globals';

import { NodeViewerComponent } from './node-viewer.component';

describe('NodeViewerComponent', () => {
  let component: NodeViewerComponent;
  let fixture: ComponentFixture<NodeViewerComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [NodeViewerComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(NodeViewerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });

  it('should not be unlockable', () => {
    expect(component).toHaveProperty('unlockable', false);
  });
});
