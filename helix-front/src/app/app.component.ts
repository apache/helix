import { Component } from '@angular/core';
import { MdDialog } from '@angular/material';

import { environment } from '../environments/environment';
import { InputDialogComponent } from './shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  footerEnabled = environment.production;

  constructor(public dialog: MdDialog) {}

  openDialog() {
    let ref = this.dialog.open(InputDialogComponent);
    ref.afterClosed().subscribe(result => {
      console.log(result);
    });
  }
}
