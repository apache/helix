import { Component, OnInit, Input } from '@angular/core';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MaterialModule } from 'app/shared/material.module';
import { ConfirmDialogComponent } from '../confirm-dialog/confirm-dialog.component';

// Wrapper component for testing approach recommended in this answer:
// https://stackoverflow.com/a/63953851/1732222
@Component({
  selector: 'hi-confirm-dialog-test',
  template: ` <button mat-flat-button (click)="openDialog()">
    Open Dialog
  </button>`,
  styleUrls: ['./confirm-dialog.component.scss'],
})
export class ConfirmDialogTestComponent {
  @Input() data: any;

  constructor(public dialog: MatDialog) {}

  public openDialog(): void {
    this.dialog.open(ConfirmDialogComponent, {
      data: this.data,
    });
  }
}

export default () => ({
  moduleMetadata: {
    _imports: [BrowserAnimationsModule, MatDialogModule, MaterialModule],
    get imports() {
      return this._imports;
    },
    set imports(value) {
      this._imports = value;
    },
    declarations: [ConfirmDialogTestComponent],
  },
  component: ConfirmDialogTestComponent,
});
