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
import { HelixListComponent } from './helix-list.component';
import { ChooserService } from '../shared/chooser.service';

describe('HelixListComponent', () => {
  let component: HelixListComponent;
  let fixture: ComponentFixture<HelixListComponent>;

  beforeEach(() => {

    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [HelixListComponent],
      providers: [ChooserService],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HelixListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
