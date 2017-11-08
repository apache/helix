import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { ChooserService } from './shared/chooser.service';
import { HelixListComponent } from './helix-list/helix-list.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  declarations: [
    HelixListComponent
  ],
  providers: [
    ChooserService
  ]
})
export class ChooserModule { }
