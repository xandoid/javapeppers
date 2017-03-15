/**
 *
 */
package com.codeondemand.javapeppers.sambal.util;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.sambal.fields.TokenField;

import java.util.ArrayList;

public abstract class SambalRecordProcessor extends RecordProcessor {

    public boolean doInitialization() {
        return initializeFields();
    }

    public RecordCapsule processRecord(RecordCapsule recin) {
        if (!initialized) {
            initialized = initializeFields();
        }

        for (TokenField field : fields) {
            recin.addDataCapsule(new DataCapsule(field.getName(), field.getNextValue()), false);
        }
        return recin;
    }

    public abstract boolean initializeFields();

    private boolean initialized = false;
    protected ArrayList<TokenField> fields = null;
}
