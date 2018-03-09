import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { TestingModule } from '../../../testing/testing.module';
import { ClusterDetailComponent } from './cluster-detail.component';

describe('ClusterDetailComponent', () => {
  let component: ClusterDetailComponent;
  let fixture: ComponentFixture<ClusterDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        TestingModule
      ],
      declarations: [
        ClusterDetailComponent
      ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ClusterDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  xit('should create', () => {
    expect(component).toBeTruthy();
  });
});
