package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

import java.util.StringTokenizer;


public class EnvironmentVariableLoader extends RecordProcessor {


    @Override
    public boolean doInitialization() {
        return true;
    }

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {

        // Load every time - since there may be cases where environment variables are manipulated
        // between records.
        if (pmap.containsKey("varlist")) {
            String varlist = (String) pmap.get("varlist");
            StringTokenizer foo = new StringTokenizer(varlist, "|");
            while (foo.hasMoreTokens()) {
                String temp = foo.nextToken();
                String val = System.getenv(temp);
                // create both upper and lower case values of this variable.
                if (val != null) {
                    input.addDataCapsule(new DataCapsule(temp, val), false);
                }
                if (props != null) {
                    props.setProperty(temp, val);
                }
            }
        }
        return input;
    }

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }
}
