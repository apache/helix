import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NO_ERRORS_SCHEMA } from '@angular/core';

import { TestingModule } from '../../../testing/testing.module';
import { WorkflowDagComponent } from './workflow-dag.component';

describe('WorkflowDagComponent', () => {
  let component: WorkflowDagComponent;
  let fixture: ComponentFixture<WorkflowDagComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ TestingModule ],
      declarations: [ WorkflowDagComponent ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkflowDagComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  xit('should create', () => {
    expect(component).toBeTruthy();
  });
});
