import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DisabledLabelComponent } from './disabled-label.component';

describe('DisabledLabelComponent', () => {
  let component: DisabledLabelComponent;
  let fixture: ComponentFixture<DisabledLabelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DisabledLabelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DisabledLabelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
