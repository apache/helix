import 'zone.js';
import 'zone.js/dist/zone-testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { DisabledLabelComponent } from './disabled-label.component';

describe('DisabledLabelComponent', () => {
  let component: DisabledLabelComponent;
  let fixture: ComponentFixture<DisabledLabelComponent>;

  beforeEach(() => {


    TestBed.configureTestingModule({
      declarations: [DisabledLabelComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DisabledLabelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
