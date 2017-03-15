/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

/**
 * Converts the contents of a RecordCapsule into a single flat delimited string
 * using the delimiter specified at initialization. This is done by
 * concatenating the data values of the DataCapsule children of the
 * RecordCapsule in sequential order.
 *
 * @author gfa
 */
public class DelimiterBuilder extends NullBuilder {

    /**
     * Constructs a DelimiterBuilder object.  This is a convenience API for
     * programmers who wish to build data flows programmatically rather than
     * by providing an XML specification file.
     *
     * @param delimiter Specifies the delimiter. If null, then the default is used,
     *                  which is a zero length string.
     */
    public DelimiterBuilder(String delimiter) {
        if (delimiter != null && delimiter.length() > 0) {
            this.delimiter = delimiter;
        }
    }

    /**
     * Expects the parameter map to contain a parameter with a name specified by
     * the constant ALEPPO_DELIMITER_NAME specified in the AleppoConstants
     * class. If it does not, then the value will remain the default value.
     */
    public boolean doInitialization() {
        boolean retval = false;
        if (pmap.containsKey(AleppoConstants.ALEPPO_DELIMITER_KEY)) {
            this.delimiter = (String) pmap.get(AleppoConstants.ALEPPO_DELIMITER_KEY);
            retval = true;
        } else {
            logger.debug("No value for delimiter found in the parameter map");
        }
        if (pmap.containsKey("timestamp")) {
            do_ts = Boolean.parseBoolean((String) pmap.get("timestamp"));
        }
        return retval;
    }


    public Object buildRecord(RecordCapsule r) {
        String retval = null;
        String prefix = "";
        if (do_ts) {
            prefix = MiscUtil.getCurrentTimeString();
            //prefix = DbUtil.currentTimestamp().toGMTString()+delimiter;
        }
        // Don't bother with anything special if there is only
        // a single field in the RecordCapsule
        if (r.getFieldCount() == 1) {
            retval = prefix + r.getField(0).getData().toString();
        } else {

            if (!headerbuilt) {
                logger.debug(buildHeader(r));
                headerbuilt = true;
            }
            StringBuffer sb = new StringBuffer();
            sb.append(prefix);
            for (int i = 0; i < r.getFieldCount(); i++) {
                if (i > 0) {
                    sb.append(delimiter);
                }
                if (!r.getField(i).isNull()) {
                    sb.append(r.getField(i).getData().toString().trim());
                }
            }
            retval = sb.toString() + "\n";
        }
        return retval;
    }

    public Object buildHeader(RecordCapsule r) {
        StringBuffer sb = new StringBuffer();
        String temp = ""; //$NON-NLS-1$
        if (do_ts) {
            sb.append("timestamp" + delimiter);
        }
        if (r != null && r.getFieldCount() > 0) {
            for (int i = 0; i < r.getFieldCount(); i++) {
                sb.append(temp + r.getField(i).getName().trim());
                temp = delimiter;
            }
            sb.append("\n");
            headerbuilt = true;
        }
        return sb.toString();
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DelimiterBuilder");

    private boolean headerbuilt = false;
    private boolean do_ts = false;
    protected String delimiter = AleppoConstants.ALEPPO_DELIMITER_DEFAULT;
}
