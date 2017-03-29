/**
 *
 */
package com.codeondemand.javapeppers.aleppo.parser;

import com.codeondemand.javapeppers.aleppo.common.FieldSpecification;
import com.codeondemand.javapeppers.aleppo.common.FlowNode;
import com.codeondemand.javapeppers.aleppo.common.KeySpecification;

import java.util.ArrayList;

public abstract class KeyedRecordParser extends FlowNode implements RecordParser {

    public boolean addField(FieldSpecification field) {
        fields.add(field);
        return true;
    }

    public boolean addKeySpecification(KeySpecification s) {
        boolean retval = false;
        if (s != null) {
            if (keyspecs == null) {
                keyspecs = new ArrayList<>();
            }
            keyspecs.add(s);
            retval = true;
        }
        return retval;
    }

    @Override
    public boolean doInitialization() {
        return true;
    }

    protected boolean isKeyField(int pos) {
        boolean retval = false;
        if (keyspecs != null) {
            for (KeySpecification keyspec : keyspecs) {
                if (pos == keyspec.getField_position()) {
                    retval = true;
                    break;
                }
            }
        }
        return retval;
    }

    protected ArrayList<FieldSpecification> fields = new ArrayList<>();

    private ArrayList<KeySpecification> keyspecs = null;
}
