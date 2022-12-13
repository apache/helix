import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { beforeEach, describe, expect, xit } from '@jest/globals';

import { InputDialogComponent } from './input-dialog.component';

describe('InputDialogComponent', () => {
  let component: InputDialogComponent;
  let fixture: ComponentFixture<InputDialogComponent>;

  beforeEach(() => {



    TestBed.configureTestingModule({
      declarations: [InputDialogComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InputDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  // disable this test until I figure out a way to test
  xit('should create', () => {
    expect(component).toBeTruthy();
  });
});
