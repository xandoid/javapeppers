/**
 *
 */
package com.codeondemand.javapeppers.aleppo.source;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import java.util.HashMap;

public class KeyedAggregateSource extends AggregateSource {

    public int getRecordKey() {
        int retval = 0;
        if (sources != null) {
            return (sources.get(0)).getRecordKey();
        }
        return retval;
    }

    @Override
    public RecordCapsule getCurrentRecord() {
        return currentRecord;
    }

    public RecordCapsule getNextRecord() {
        RecordCapsule retval = null;
        logger.debug("getNextRecord called");
        if (sources != null) {
            if ((retval = sources.get(0).getNextRecord()) != null) {
                logger.debug(retval.toString());
                String key = retval.getKeyString();
                for (int idx = 1; idx < sources.size(); idx++) {
                    if (!recs.containsKey(idx)) {
                        recs.put(idx, sources.get(idx).getNextRecord());
                        logger.debug(recs.get(idx).toString());
                    } else {
                        logger.debug("Keeping record:" + recs.get(idx));
                    }
                    if (hdrs.get(idx) == null) {
                        for (int i = 0; i < recs.get(idx).getFieldCount(); i++) {
                            RecordCapsule rec = recs.get(idx);
                            RecordCapsule hdr = new RecordCapsule("header", "header");
                            DataCapsule dc = rec.getField(i);
                            hdr.addDataCapsule(new DataCapsule(dc.getName(), null), rec.isKey(dc.getName()));
                            hdrs.put(idx, hdr);
                        }
                    }
                }

                for (int idx = 1; idx < sources.size(); idx++) {
                    if (recs.containsKey(idx) && recs.get(idx).getKeyString().equals(key)) {
                        retval = combine(retval, recs.get(idx));
                        recs.remove(idx);
                        logger.debug("Removed record for " + idx + ":" + recs.get(idx));
                    } else {
                        retval = combineNull(retval, hdrs.get(idx));
                    }
                }
            }
        }
        logger.debug("Returning next record: " + retval);
        currentRecord = retval;
        return retval;
    }

    protected RecordCapsule combine(RecordCapsule current, RecordCapsule append) {
        RecordCapsule retval = current;

        if (retval == null) {
            retval = append;
        } else {
            for (int i = 0; i < append.getFieldCount(); i++) {
                if (!append.isKey(append.getField(i).getName())) {
                    // Don't add fields with the same name
                    if (current.getField(append.getField(i).getName()) == null) {
                        current.addDataCapsule(append.getField(i), false);
                    }
                }
            }
        }
        return retval;
    }

    protected RecordCapsule combineNull(RecordCapsule current, RecordCapsule append) {

        for (int i = 0; i < append.getFieldCount(); i++) {
            current.addDataCapsule(new DataCapsule(append.getField(i).getName(), null), false);
        }

        return current;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("KeyedAggregateSource");

    private HashMap<Integer, RecordCapsule> hdrs = new HashMap<Integer, RecordCapsule>();
    private HashMap<Integer, RecordCapsule> recs = new HashMap<Integer, RecordCapsule>();
    private RecordCapsule currentRecord = null;
}
