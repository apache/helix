import * as _ from 'lodash';

interface SimpleFieldObject {
  name: string;
  value: string;
}

interface ListFieldObject {
  name: string;
  value: SimpleFieldObject[];
}

interface MapFieldObject {
  name: string;
  value: SimpleFieldObject[];
}

interface Payload {
  id: string;
  simpleFields?: any;
  listFields?: any;
  mapFields?: any;
}

// This is a typical Helix Node definition
export class Node {
  id: string;
  simpleFields: SimpleFieldObject[] = [];
  listFields: ListFieldObject[] = [];
  mapFields: MapFieldObject[] = [];

  constructor(obj: any) {
    if (obj != null) {
      this.id = obj.id;
      this.simpleFields = this.keyValueToArray(obj.simpleFields);

      _.forOwn(obj['listFields'], (v, k) => {
        this.listFields.push(<ListFieldObject>{
          name: k,
          value: _.map(
            v,
            (item) =>
              <SimpleFieldObject>{
                value: item,
              }
          ),
        });
      });

      _.forOwn(obj['mapFields'], (v, k) => {
        this.mapFields.push(<MapFieldObject>{
          name: k.trim(),
          value: this.keyValueToArray(v),
        });
      });
    }
  }

  public appendSimpleField(name: string, value: string) {
    this.simpleFields.push(<SimpleFieldObject>{
      name,
      value,
    });
  }

  public appendMapField(key: string, name: string, value: string) {
    const index = _.findIndex(this.mapFields, { name: key });
    if (index >= 0) {
      this.mapFields[index].value.push(<SimpleFieldObject>{
        name: name.trim(),
        value,
      });
    } else {
      this.mapFields.push(<MapFieldObject>{
        name: key,
        value: [
          <SimpleFieldObject>{
            name: name.trim(),
            value,
          },
        ],
      });
    }
  }

  public json(id: string): string {
    const obj: Payload = {
      id,
    };

    if (this?.simpleFields.length > 0) {
      obj.simpleFields = {};
      _.forEach(this.simpleFields, (item: SimpleFieldObject) => {
        obj.simpleFields[item.name] = item.value;
      });
    }

    if (this?.listFields.length > 0) {
      obj.listFields = {};
      _.forEach(this.listFields, (item: ListFieldObject) => {
        obj.listFields[item.name] = [];
        _.forEach(item.value, (subItem: SimpleFieldObject) => {
          obj.listFields[item.name].push(subItem.value);
        });
      });
    }

    if (this?.mapFields.length > 0) {
      obj.mapFields = {};
      _.forEach(this.mapFields, (item: MapFieldObject) => {
        obj.mapFields[item.name.trim()] = item.value ? {} : null;
        _.forEach(item.value, (subItem: SimpleFieldObject) => {
          // if the value is a string that contains all digits, parse it to a number
          let parsedValue: string | number = subItem.value;
          if (
            typeof subItem.value === 'string' &&
            /^\d+$/.test(subItem.value)
          ) {
            parsedValue = Number(subItem.value);
          }

          obj.mapFields[item.name.trim()][subItem.name] = parsedValue;
        });
      });
    }

    return JSON.stringify(obj);
  }

  // Converting raw simpleFields to SimpleFieldObject[]
  private keyValueToArray(obj: Object): SimpleFieldObject[] {
    const result: SimpleFieldObject[] = [];
    for (const k in obj) {
      if (obj.hasOwnProperty(k)) {
        result.push(<SimpleFieldObject>{
          name: k,
          value: obj[k],
        });
      }
    }
    return result;
  }
}
