import {
  Component,
  OnInit,
  Input,
  Output,
  ViewChild,
  ElementRef,
  EventEmitter,
} from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';

@Component({
  selector: 'hi-input-inline',
  templateUrl: './input-inline.component.html',
  styleUrls: ['./input-inline.component.scss'],
})
export class InputInlineComponent implements ControlValueAccessor, OnInit {
  @ViewChild('inputControl', { static: true }) inputControl: ElementRef;

  @Output('update') change: EventEmitter<string> = new EventEmitter<string>();

  @Input() label = '';
  @Input() min = -9999;
  @Input() max = 99999999;
  @Input() minlength = 0;
  @Input() maxlength = 2555;
  @Input() type = 'text';
  @Input() required = false;
  @Input() pattern: string = null;
  @Input() errorLabel = 'Invalid input value';
  @Input() editLabel = 'Click to edit';
  @Input() disabled = false;

  editing = false;

  private _value = '';

  private lastValue = '';

  // Required forControlValueAccessor interface
  public onChange: any = Function.prototype;
  public onTouched: any = Function.prototype;
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
  @Input() focus: Function = (_) => {};
  @Input() blur: Function = (_) => {};
  public registerOnChange(fn: (_: any) => {}): void {
    this.onChange = fn;
  }
  public registerOnTouched(fn: () => {}): void {
    this.onTouched = fn;
  }
  writeValue(value: any) {
    this._value = value;
  }

  constructor() {}

  ngOnInit() {}

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
    setTimeout((_) => {
      this.inputControl?.nativeElement.focus();
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
}
