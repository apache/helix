import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';

import { ResourceService } from './shared/resource.service';
import { ResourceResolver } from './shared/resource.resolver';
import { ResourceListComponent } from './resource-list/resource-list.component';
import { ResourceDetailComponent } from './resource-detail/resource-detail.component';

@NgModule({
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule
  ],
  providers: [
    ResourceService,
    ResourceResolver
  ],
  declarations: [ResourceListComponent, ResourceDetailComponent]
})
export class ResourceModule { }
