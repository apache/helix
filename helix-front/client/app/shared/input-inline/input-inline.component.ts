import { Component, OnInit, Input, Output, ViewChild, ElementRef, EventEmitter } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';

@Component({
  selector: 'hi-input-inline',
  templateUrl: './input-inline.component.html',
  styleUrls: ['./input-inline.component.scss']
})
export class InputInlineComponent implements ControlValueAccessor, OnInit {

  @ViewChild('inputControl') inputControl: ElementRef;

  @Output('update') change: EventEmitter<string> = new EventEmitter<string>();

  @Input() label: string = '';
  @Input() min: number = -9999;
  @Input() max: number = 99999999;
  @Input() minlength: number = 0;
  @Input() maxlength: number = 2555;
  @Input() type: string = 'text';
  @Input() required: boolean = false;
  @Input() focus: Function = _ => { };
  @Input() blur: Function = _ => { };
  @Input() pattern: string = null;
  @Input() errorLabel: string = 'Invalid input value';
  @Input() editLabel: string = 'Click to edit';
  @Input() disabled: boolean = false;

  private editing: boolean = false;

  private _value: string = '';
  @Input()
  get value(): any {
    return this._value;
  }
  set value(v: any) {
    if (v !== this._value) {
      this._value = v;
      this.onChange(v);
    }
  }

  private lastValue: string = '';

  // Required forControlValueAccessor interface
  public onChange: any = Function.prototype;
  public onTouched: any = Function.prototype;
  public registerOnChange(fn: (_: any) => {}): void { this.onChange = fn; }
  public registerOnTouched(fn: () => {}): void { this.onTouched = fn; };
  writeValue(value: any) {
    this._value = value;
  }

  constructor() { }

  ngOnInit() {
  }

  hasError() {
    const exp = new RegExp(this.pattern);

    if (!this.value) {
      return this.required;
    }

    if (this.pattern && !exp.test(this.value)) {
      return true;
    }

    return false;
  }

  edit(value) {
    if (this.disabled) {
      return;
    }

    this.lastValue = value;
    this.editing = true;
    setTimeout(_ => {
      this.inputControl.nativeElement.focus();
    });
  }

  onBlur($event: Event) {
    if (this.hasError()) {
      return false;
    }

    // this.blur();
    this.editing = false;
    this.change.emit(this.value);
  }

  cancel() {
    this._value = this.lastValue;
    this.editing = false;
  }

  keyup($event) {
    if ($event.key === 'Escape' || $event.keyCode == 27) {
      this.cancel();
    }
  }

}
