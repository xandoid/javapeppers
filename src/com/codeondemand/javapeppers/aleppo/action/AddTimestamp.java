package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

import java.sql.Timestamp;
import java.util.GregorianCalendar;

public class AddTimestamp extends RecordProcessor {

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {

        if (!initialized) {
            initialized = doInitialization();
        }
        Timestamp ts = new Timestamp(new GregorianCalendar().getTimeInMillis());
        input.addDataCapsule(new DataCapsule(label, ts.toString()), false);
        return input;
    }

    @Override
    public boolean doInitialization() {
        if (pmap.containsKey("ts_label")) {
            label = (String) pmap.get("ts_label");
        }
        return true;
    }

    private String label = "timestamp";
    private boolean initialized = false;


    public void done() {
        initialized = false;
    }
}
