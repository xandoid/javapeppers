/**
 *
 */
package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;

/**
 * A convenience class for doing case conversions of records being processed.
 * This component allows objects to be converted to upper or lower case. You may
 * also specify no conversion. If this class is not initialized properly it will
 * return null strings for the converted objects.
 * <p>
 * This class current supports conversions of records that are either single
 * Strings or String arrays.
 *
 * @author gfa
 */
public class CaseTransform extends RecordTransform {

    public boolean doInitialization() {
        boolean retval = true;

        if (pmap.containsKey(AleppoConstants.ALEPPO_TRANSFORM_CONVERSION_KEY)) {
            operation = Integer.parseInt((String) pmap.get(AleppoConstants.ALEPPO_TRANSFORM_CONVERSION_KEY));
            if (operation < MIN_OPTION || operation > MAX_OPTION) {
                logger.error("Invalid case conversion operation value passed");
                retval = false;
            }
        } else {
            logger.debug("Using default case conversion operation");
        }

        if (pmap.containsKey(AleppoConstants.ALEPPO_TRANSFORM_XFORMNAMES_KEY)) {
            String[] foo = ((String) pmap.get(AleppoConstants.ALEPPO_TRANSFORM_XFORMNAMES_KEY)).split(":");
            for (int i = 0; i < foo.length; i++) {
                names.add(foo[i]);
                logger.debug("XFORMNAME: " + foo[i]);
            }
        }

        return retval;
    }

    @Override
    /*
	 * This method will do case conversions on String objects and String[]
	 * objects. The type of case conversion should have been specified during
	 * the initialization step of this component.
	 * 
	 * @param record The incoming object to convert.
	 * 
	 * @return A converted object or a null. If the incoming object is a
	 * String[] then it may return a null for all elements if an invalid
	 * operation was specified.
	 */ public RecordCapsule doTransform(RecordCapsule record) {
        if (!initialized) {
            initialized = doInitialization();
        }
        for (int i = 0; i < record.getFieldCount(); i++) {
            DataCapsule d = record.getField(i);
            if (names.size() == 0 || names.contains(d.getName())) {
                if (d.getData() instanceof String) {
                    d.setData(changeCase((String) d.getData(), operation));
                } else {
                    logger.error("Case conversion requested for non-String object.");
                }
            }
        }
        return record;
    }

    /*
     * Converts a String to a String using the specified operation.
     *
     * @param input The String to convert;
     *
     * @param operation The type of case conversion to perform.
     *
     * @return Returns a converted string (may be a no-op) or a null;
     */
    private static String changeCase(String input, int operation) {
        String retval = null;
        logger.debug("Incoming string to convert: " + input + " using operation " + operation);
        if (input != null) {
            switch (operation) {
                case TO_UPPER_CASE:
                    retval = input.toUpperCase();
                    break;
                case TO_LOWER_CASE:
                    retval = input.toLowerCase();
                    break;
                case NO_CONVERSION:
                    retval = input;
                    break;

                // If a valid conversion was not specified, return null
                default:
                    logger.debug("Invalid case conversion operation specified:" //$NON-NLS-1$
                            + operation);
                    retval = null;
            }
        }
        logger.debug("Converted value: " + retval);
        return retval;

    }

    public final static int MIN_OPTION = 0;
    public final static int NO_CONVERSION = 0;
    public final static int TO_UPPER_CASE = 1;
    public final static int TO_LOWER_CASE = 2;
    public final static int MAX_OPTION = 2;

    private ArrayList<String> names = new ArrayList<String>();

    // Note: This is illegal so if not set by application, this
    private int defaultConversion = NO_CONVERSION;
    private Integer operation = defaultConversion;
    private boolean initialized = false;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("CaseTransform");

    public void done() {
        initialized = false;
    }

}
