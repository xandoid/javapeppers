/**
 *
 */
package com.codeondemand.javapeppers.aleppo.common;

/**
 * Simple container to hold the position and type of a key in
 * a record structure.
 *
 * @author gfa
 */
public class KeySpecification {

    public KeySpecification(int pos, int type) {
        this.field_position = pos;
        this.field_type = type;
    }

    public KeySpecification(String name, int pos, int type) {
        this.name = name;
        this.field_position = pos;
        this.field_type = type;
    }

    public int getField_position() {
        return field_position;
    }

    public void setField_position(int field_position) {
        this.field_position = field_position;
    }

    public int getField_type() {
        return field_type;
    }

    public void setField_type(int field_type) {
        this.field_type = field_type;
    }

    protected String name = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected int field_position = 0;
    protected int field_type = java.sql.Types.NULL;
}
