import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { HttpModule } from '@angular/http';

import { ResourceService } from '../shared/resource.service';
import { ResourceDetailForInstanceComponent } from './resource-detail-for-instance.component';

describe('ResourceDetailForInstanceComponent', () => {
  let component: ResourceDetailForInstanceComponent;
  let fixture: ComponentFixture<ResourceDetailForInstanceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ HttpModule ],
      declarations: [ ResourceDetailForInstanceComponent ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ],
      providers: [ ResourceService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceDetailForInstanceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
