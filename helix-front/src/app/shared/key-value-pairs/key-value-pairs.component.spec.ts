import 'zone.js';
import 'zone.js/dist/zone-testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { KeyValuePairsComponent } from './key-value-pairs.component';

describe('KeyValuePairsComponent', () => {
  let component: KeyValuePairsComponent;
  let fixture: ComponentFixture<KeyValuePairsComponent>;

  beforeEach(() => {


    TestBed.configureTestingModule({
      declarations: [KeyValuePairsComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(KeyValuePairsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
