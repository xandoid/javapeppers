package com.codeondemand.javapeppers.aleppo.filter;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import java.util.Random;

/**
 * The RandomPercentageFilter allows some percentage of records (selected
 * randomly) to pass through unchanged and blocks the rest of the records from
 * passing through.
 *
 * @author gfa
 * @see com.codeondemand.javapeppers.aleppo.filter.BlockingFilter
 */
public class RandomPercentageFilter extends RecordFilter {

    /**
     * Initializes the percentage of records that will be passed through the
     * filter. The value should have been included in the parameter map passed
     * to the initialization method.
     */
    public boolean doInitialization() {
        boolean retval = false;
        if (pmap.containsKey(AleppoConstants.ALEPPO_FILTER_PCT_KEY)) {

            filterPct = Integer.parseInt((String) pmap.get(AleppoConstants.ALEPPO_FILTER_PCT_KEY));
            if (filterPct >= 0 && filterPct <= 100) {
                logger.debug(AleppoMessages.getString("RandomPercentageFilter.0") + filterPct); //$NON-NLS-1$
                initialized = true;
                retval = true;
            } else {
                logger.error(AleppoMessages.getString("RandomPercentageFilter.1") + filterPct); //$NON-NLS-1$
                retval = false;
            }
        }
        return retval;
    }

    /**
     * Tries to maintain a 'passing' rate of the percentage specified during
     * initialization. Since there is randomness in the determination of
     * filtering each record, it will only approximate the filter percentage for
     * small numbers of records. Note: Successive runs of records through this
     * filtration node will not result in the same records passing the filter
     * since the contents of the record have nothing to do with whether or not
     * it is filtered.
     * <p>
     * Perhaps a TO-DO would be to modify the algorithm to run a hash on the
     * RecordCapsule and do a modulo 100 to compare against the 0-100 pct value.
     */
    @Override
    protected synchronized RecordCapsule filterRecord(RecordCapsule input) {

        RecordCapsule retval = input;
        if (input != null && initialized) {
            if (rangen != null) {
                double adjust = (objectsPassed - objectsNotPassed) * 2.0 / ((objectsPassed + objectsNotPassed) * 1.0);
                if ((filterPct < 90) && rangen.nextDouble() + adjust > filterPct / 100.0) {
                    logger.debug(AleppoMessages.getString("RandomPercentageFilter.2") + input.toString()); //$NON-NLS-1$
                    retval = null;
                } else {
                    logger.debug(AleppoMessages.getString("RandomPercentageFilter.3") + input.toString()); //$NON-NLS-1$
                }
            }
        }
        return retval;
    }

    // Set default value to allow all records to pass through.
    protected int filterPct = 100;

    protected Random rangen = new Random();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RandomPercentageFilter");

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }

}
