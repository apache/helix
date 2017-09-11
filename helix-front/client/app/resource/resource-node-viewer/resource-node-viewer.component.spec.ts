import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { TestingModule } from '../../../testing/testing.module';
import { ResourceNodeViewerComponent } from './resource-node-viewer.component';
import { ResourceService } from '../shared/resource.service';

describe('ResourceNodeViewerComponent', () => {
  let component: ResourceNodeViewerComponent;
  let fixture: ComponentFixture<ResourceNodeViewerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ TestingModule ],
      declarations: [ ResourceNodeViewerComponent ],
      providers: [ ResourceService ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceNodeViewerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
