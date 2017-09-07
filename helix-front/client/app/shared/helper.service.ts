import { Injectable } from '@angular/core';
import { MdDialog, MdSnackBar } from '@angular/material';

import { AlertDialogComponent } from './dialog/alert-dialog/alert-dialog.component';

@Injectable()
export class HelperService {

  constructor(
    protected snackBar: MdSnackBar,
    protected dialog: MdDialog
  ) { }

  showError(message: string) {
    this.dialog.open(AlertDialogComponent, {
      data: {
        title: 'Error',
        message: message
      }
    });
  }

  showSnackBar(message: string) {
    this.snackBar.open(message, 'OK', {
      duration: 2000,
    });
  }

}
