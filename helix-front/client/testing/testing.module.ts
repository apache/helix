import { NgModule } from '@angular/core';
import { HttpModule } from '@angular/http';
import { MaterialModule } from '../app/shared/material.module';
import { RouterTestingModule } from '@angular/router/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

import { HelperService } from '../app/shared/helper.service';
import { HelperServiceStub } from './stubs';

@NgModule({
  imports: [
    HttpModule,
    MaterialModule,
    RouterTestingModule,
    NoopAnimationsModule
  ],
  providers: [
    {
      provide: HelperService,
      useValue: HelperServiceStub
    }
  ],
  exports: [
    HttpModule,
    MaterialModule,
    RouterTestingModule,
    NoopAnimationsModule
  ]
})
export class TestingModule { }
