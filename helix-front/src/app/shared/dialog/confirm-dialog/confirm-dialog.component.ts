import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
  selector: 'hi-confirm-dialog',
  templateUrl: './confirm-dialog.component.html',
  styleUrls: ['./confirm-dialog.component.scss'],
})
export class ConfirmDialogComponent implements OnInit {
  title: string;
  message: string;
  confirmButtonText: string;

  constructor(
    @Inject(MAT_DIALOG_DATA) protected data: any,
    protected dialogRef: MatDialogRef<ConfirmDialogComponent>
  ) {}

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Confirmation';
    this.message =
      (this.data && this.data.message) || 'Are you sure about this?';
    this.confirmButtonText =
      (this.data && this.data.confirmButtonText) || 'Continue';
  }

  onConfirm() {
    this.dialogRef.close(true);
  }

  onCancel() {
    this.dialogRef.close();
  }
}
