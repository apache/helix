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
          value: _.map(v, item => {
            return <SimpleFieldObject>{
              value: item
            };
          })
        });
      });

      _.forOwn(obj['mapFields'], (v, k) => {
        this.mapFields.push(<MapFieldObject>{
          name: k,
          value: this.keyValueToArray(v)
        });
      });
    }
  }

  public appendSimpleField(name: string, value: string) {
    this.simpleFields.push(<SimpleFieldObject>{
      name: name,
      value: value
    });
  }

  public appendMapField(key: string, name: string, value: string) {
    const index = _.findIndex(this.mapFields, {'name': key});
    if (index >= 0) {
      this.mapFields[index].value.push(<SimpleFieldObject>{
        name: name,
        value: value
      });
    } else {
      this.mapFields.push(<MapFieldObject>{
        name: key,
        value: [<SimpleFieldObject>{
          name: name,
          value: value
        }]
      });
    }
  }

  public json(id: string): string {
    let obj = {
      id: id,
      simpleFields: {},
      listFields: {},
      mapFields: {}
    };

    _.forEach(this.simpleFields, (item: SimpleFieldObject) => {
      obj.simpleFields[item.name] = item.value;
    });

    _.forEach(this.listFields, (item: ListFieldObject) => {
      obj.listFields[item.name] = [];
      _.forEach(item.value, (subItem: SimpleFieldObject) => {
        obj.listFields[item.name].push(subItem.value);
      });
    });

    _.forEach(this.mapFields, (item: MapFieldObject) => {
      obj.mapFields[item.name] = item.value ? {} : null;
      _.forEach(item.value, (subItem: SimpleFieldObject) => {
        obj.mapFields[item.name][subItem.name] = subItem.value;
      });
    });

    return JSON.stringify(obj);
  }

  // Converting raw simpleFields to SimpleFieldObject[]
  private keyValueToArray(obj: Object): SimpleFieldObject[] {
    const result: SimpleFieldObject[] = [];
    for (const k in obj) {
      if (obj.hasOwnProperty(k)) {
        result.push(<SimpleFieldObject>{
          name: k,
          value: obj[k]
        });
      }
    }
    return result;
  }
}
