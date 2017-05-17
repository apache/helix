import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ResourceListComponent } from './resource-list/resource-list.component';
import { MaterialModule } from '@angular/material';
import { ResourceDetailComponent } from './resource-detail/resource-detail.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule
  ],
  declarations: [ResourceListComponent, ResourceDetailComponent]
})
export class ResourceModule { }
