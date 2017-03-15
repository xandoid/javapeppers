package com.codeondemand.javapeppers.sambal.fields;

public abstract class DataField {


    public abstract String getName();

    public abstract boolean isNumeric();

    public abstract Object getNextValue();

    public Object getCurrentValue() {
        if (current_value == null) {
            current_value = getNextValue();
        }
        return current_value;
    }

    protected Object current_value = null;

}
