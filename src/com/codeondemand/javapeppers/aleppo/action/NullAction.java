package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

public class NullAction extends RecordProcessor {

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return input;
    }

    @Override
    public boolean doInitialization() {
        if (pmap.containsKey("delay")) {
            delay = Long.parseLong((String) pmap.get("delay"));
        }
        return true;
    }

    private long delay = 0L;

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }
}
