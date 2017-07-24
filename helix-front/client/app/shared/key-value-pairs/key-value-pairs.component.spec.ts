import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { KeyValuePairsComponent } from './key-value-pairs.component';

describe('KeyValuePairsComponent', () => {
  let component: KeyValuePairsComponent;
  let fixture: ComponentFixture<KeyValuePairsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ KeyValuePairsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(KeyValuePairsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
