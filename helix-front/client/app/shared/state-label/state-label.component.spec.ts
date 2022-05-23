import { waitForAsync, ComponentFixture, TestBed } from '@angular/core/testing';

import { StateLabelComponent } from './state-label.component';

describe('StateLabelComponent', () => {
  let component: StateLabelComponent;
  let fixture: ComponentFixture<StateLabelComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [ StateLabelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StateLabelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
