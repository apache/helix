import { Injectable } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';

import { AlertDialogComponent } from './dialog/alert-dialog/alert-dialog.component';
import { ConfirmDialogComponent } from './dialog/confirm-dialog/confirm-dialog.component';

@Injectable()
export class HelperService {
  constructor(protected snackBar: MatSnackBar, protected dialog: MatDialog) {}

  private parseMessage(message: string | object) {
    return typeof message === 'string'
      ? message
      : JSON.stringify(message, null, 2);
  }

  showError(message: string | object) {
    this.dialog.open(AlertDialogComponent, {
      data: {
        title: 'Error',
        message: this.parseMessage(message),
      },
    });
  }

  showSnackBar(message: string | object) {
    this.snackBar.open(this.parseMessage(message), 'OK', {
      duration: 2000,
    });
  }

  showConfirmation(
    message: string | object,
    title?: string,
    confirmButtonText?: string
  ) {
    return this.dialog
      .open(ConfirmDialogComponent, {
        data: {
          message: this.parseMessage(message),
          title,
          confirmButtonText,
        },
      })
      .afterClosed()
      .toPromise();
  }
}
