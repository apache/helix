package org.apache.helix.ui.api;

public class ConfigTableRow implements Comparable<ConfigTableRow> {
    private final String scope;
    private final String entity;
    private final String name;
    private final String value;

    public ConfigTableRow(String scope,
                          String entity,
                          String name,
                          String value) throws Exception {
        this.scope = scope;
        this.entity = entity;
        this.name = name;
        this.value = value;
    }

    public String getScope() {
        return scope;
    }

    public String getEntity() {
        return entity;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int compareTo(ConfigTableRow o) {
        int nameResult = name.compareTo(o.getName());
        if (nameResult != 0) {
            return nameResult;
        }

        int valueResult = value.compareTo(o.getValue());
        if (valueResult != 0) {
            return valueResult;
        }

        return 0;
    }
}
