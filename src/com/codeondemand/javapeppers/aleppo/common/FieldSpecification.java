/**
 *
 */
package com.codeondemand.javapeppers.aleppo.common;

/**
 * This whole class needs to be revisited and probably dumped.
 *
 * @author gfa
 */
public class FieldSpecification {

    /**
     * Creates a named and ordered FieldSpecification object of a
     * specific type.
     *
     * @param name   The name of the field
     * @param number The positional number of the field
     * @param type   Indicates the type (java.sql.Types) of the field
     */
    public FieldSpecification(String name, int number, int type) {
        setType(type);
        setName(name);
        setField_num(number);
    }

    /**
     * Creates an unamed FieldSpecification object.
     *
     * @param start    Indicates the starting position of the field in the record.
     * @param length   Indicates the ending position of the field in the record.
     * @param type     Indicates the type (java.sql.Types) of the field
     * @param isBinary Indicates if this field contains binary data.
     */
    public FieldSpecification(int start, int length, int type, boolean isBinary) {
        setStart_pos(start);
        setLength(length);
        setType(type);

        this.setBinary(isBinary);
    }

    public FieldSpecification(int start, int length, int type) {
        setStart_pos(start);
        setLength(length);
        setType(type);
    }

    public FieldSpecification(int start, int length) {
        setStart_pos(start);
        setLength(length);
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setTypeName(String tname) {
        this.typeName = tname;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public int getStart_pos() {
        return start_pos;
    }

    public void setStart_pos(int start_pos) {
        this.start_pos = start_pos;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isBinary() {
        return isBinary;
    }

    public void setBinary(boolean isBinary) {
        this.isBinary = isBinary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private int start_pos = 0;
    private int length = 0;
    private int field_num = -1;
    private String typeName = "";

    public int getField_num() {
        return field_num;
    }

    public void setField_num(int field_num) {
        this.field_num = field_num;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean isKey) {
        this.isKey = isKey;
    }

    private boolean isKey = false;
    private boolean isBinary = false;
    private String name = "";
    private int type = java.sql.Types.CHAR;
}
