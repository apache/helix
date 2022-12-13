import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { FlexLayoutModule } from '@angular/flex-layout';
import { beforeAll, beforeEach, describe, expect, it } from '@jest/globals';

import { TestingModule } from '../../testing/testing.module';
import { ClusterComponent } from './cluster.component';

// mock window.matchMedia
window.matchMedia =
  window.matchMedia ||
  function () {
    return {
      matches: false,
      addListener: function () {},
      removeListener: function () {},
      addEventListener: function () {},
      removeEventListener: function () {},
      media: '',
      onchange: function () {},
      dispatchEvent: function () {
        return true;
      },
    };
  };

describe('ClusterComponent', () => {
  let component: ClusterComponent;
  let fixture: ComponentFixture<ClusterComponent>;

  beforeAll(() => {

  });

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [FlexLayoutModule, TestingModule],
      declarations: [ClusterComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ClusterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });

  it('should contain cluster list', () => {
    fixture.detectChanges();
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('hi-cluster-list')).toBeDefined();
  });
});
