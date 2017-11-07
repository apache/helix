import { NgModule } from '@angular/core';
import { HttpModule } from '@angular/http';
import { MaterialModule } from '@angular/material';
import { RouterTestingModule } from '@angular/router/testing';

import { HelperService } from '../app/shared/helper.service';
import { HelperServiceStub } from './stubs';

@NgModule({
  imports: [
    HttpModule,
    MaterialModule,
    RouterTestingModule
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
    RouterTestingModule
  ]
})
export class TestingModule { }
