import { Component, OnInit, Inject } from '@angular/core';
import { MdDialogRef } from '@angular/material';
import { MD_DIALOG_DATA } from '@angular/material';

@Component({
  selector: 'hi-confirm-dialog',
  templateUrl: './confirm-dialog.component.html',
  styleUrls: ['./confirm-dialog.component.scss']
})
export class ConfirmDialogComponent implements OnInit {

  title: string;
  message: string;

  constructor(
    @Inject(MD_DIALOG_DATA) protected data: any,
    protected dialogRef: MdDialogRef<ConfirmDialogComponent>
  ) { }

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Confirmation';
    this.message = (this.data && this.data.message) || 'Are you sure about this?';
  }

  onConfirm() {
    this.dialogRef.close(true);
  }

  onCancel() {
    this.dialogRef.close();
  }

}
