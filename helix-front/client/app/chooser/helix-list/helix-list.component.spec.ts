import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { MaterialModule } from '@angular/material';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpModule } from '@angular/http';

import { HelixListComponent } from './helix-list.component';
import { ChooserService } from '../shared/chooser.service';

describe('HelixListComponent', () => {
  let component: HelixListComponent;
  let fixture: ComponentFixture<HelixListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        MaterialModule,
        RouterTestingModule,
        HttpModule
      ],
      declarations: [ HelixListComponent ],
      providers: [ ChooserService ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HelixListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
