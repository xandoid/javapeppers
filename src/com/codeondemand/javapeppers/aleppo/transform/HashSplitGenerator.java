package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

public class HashSplitGenerator extends RecordTransform {

    @Override
    public RecordCapsule doTransform(RecordCapsule input) {
        if (!initialized) {
            initialized = doInitialization();
        }
        if (initialized) {
            if (input.getField(field) != null && !input.getField(field).isNull()) {
                String value = input.getField(field).getData().toString();
                int start = 0;
                int end = value.length();
                if (pmap.containsKey("start") && pmap.containsKey("end")) {
                    start = Integer.parseInt((String) pmap.get("start"));
                    int temp_end = Integer.parseInt((String) pmap.get("end"));
                    if (end > temp_end) {
                        end = temp_end;
                    }
                }
                byte[] hex = MiscUtil.encodeHexBytes(value.substring(start, end).getBytes());
                int hashtotal = 0;
                for (int i = 0; i < hex.length; i++) {
                    hashtotal += (int) hex[i] + i;
                }
                int split = hashtotal % splits;
                //split = (split * 997) % splits;
                input.addDataCapsule(new DataCapsule("split", split), false);
            }
        }

        return input;
    }

    @Override
    public boolean doInitialization() {
        boolean retval = true;
        if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD)) {
            field = (String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD);

        } else {
            logger.error("Unable to initialize HashSplitGenerator: Missing field parameter.");
            retval = false;
        }
        if (pmap.containsKey("splits")) {
            splits = Integer.parseInt((String) pmap.get("splits"));
        } else {
            logger.error("Unable to initialize HashSplitGenerator: Missing splits parameter.");
            retval = false;
        }
        return retval;
    }

    protected String field = null;
    protected int splits = 0;
    protected boolean initialized = false;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("HashSplitGenerator");

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }
}
