import { Component, OnInit, Inject } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material';

@Component({
  selector: 'hi-alert-dialog',
  templateUrl: './alert-dialog.component.html',
  styleUrls: ['./alert-dialog.component.scss']
})
export class AlertDialogComponent implements OnInit {

  title: string;
  message: string;

  constructor(
    @Inject(MAT_DIALOG_DATA) protected data: any
  ) { }

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Alert';
    this.message = (this.data && this.data.message) || 'Something happened.';
  }

}
