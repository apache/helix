import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { InputInlineComponent } from './input-inline.component';

describe('InputInlineComponent', () => {
  let component: InputInlineComponent;
  let fixture: ComponentFixture<InputInlineComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InputInlineComponent ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InputInlineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
