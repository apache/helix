import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef } from '@angular/material';
import { MAT_DIALOG_DATA } from '@angular/material';
import { FormControl, FormGroup } from '@angular/forms';

@Component({
  selector: 'hi-input-dialog',
  templateUrl: './input-dialog.component.html',
  styleUrls: ['./input-dialog.component.scss']
})
export class InputDialogComponent implements OnInit {

  title: string;
  message: string;
  // values: any[];
  myForm: FormGroup;

  constructor(
    @Inject(MAT_DIALOG_DATA) protected data: any,
    protected dialogRef: MatDialogRef<InputDialogComponent>
  ) { }

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Input';
    this.message = (this.data && this.data.message) || 'Please enter:';
    // this.values = (this.data && this.data.values) || {
    //   'input': {
    //     label: 'Anything you want',
    //     type: 'input'
    //   }
    // };
    this.myForm = new FormGroup({
      values: new FormControl((this.data && this.data.values) || {
        'input': {
          label: 'Anything you want',
          type: 'input'
        }
      }) 
    })
    console.log(this.myForm)
  }

  // onSubmit() {
  //   this.dialogRef.close(this.values);
  // }

  onSubmit(form: FormGroup) {
    this.dialogRef.close(form.value.values);
    console.log('Form Valid?', form.valid)
    console.log('Title', form.value.title)
    console.log('Message', form.value.message)
    console.log('Values', JSON.stringify(form.value.values))
  }

  onCancel() {
    this.dialogRef.close();
  }

  getKeys(obj: any) {
    return Object.keys(obj);
  }

}
