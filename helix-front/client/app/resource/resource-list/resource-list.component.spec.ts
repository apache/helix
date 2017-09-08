import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { HttpModule } from '@angular/http';
import { RouterTestingModule } from '@angular/router/testing';

import { ResourceListComponent } from './resource-list.component';
import { ResourceService } from '../shared/resource.service';
import { HelperService } from '../../shared/helper.service';

describe('ResourceListComponent', () => {
  let component: ResourceListComponent;
  let fixture: ComponentFixture<ResourceListComponent>;

  beforeEach(async(() => {
    // stub HelperService for test purpose
    const helperServiceStub = {
      showError: (message: string) => {},
      showSnackBar: (message: string) => {}
    };

    TestBed.configureTestingModule({
      imports: [
        HttpModule,
        RouterTestingModule
      ],
      declarations: [ ResourceListComponent ],
      providers: [
        ResourceService,
        {
          provide: HelperService,
          useValue: helperServiceStub
        }
      ],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
