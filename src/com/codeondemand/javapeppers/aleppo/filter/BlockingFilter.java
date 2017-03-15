/**
 *
 */

package com.codeondemand.javapeppers.aleppo.filter;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

/**
 * This is a simple filter that just acts like a black hole and does
 * not allow any objects to pass through.  This can be useful for testing
 * sources and other filters.
 *
 * @author gfa
 */
public final class BlockingFilter extends RandomPercentageFilter {


    /**
     * There is really no current functionality when this method is called.  It is
     * implemented for compatibility purposes.
     *
     * @return Always returns true.
     */
    public boolean doInitialization() {
        initialized = true;
        logger.debug(AleppoMessages.getString("BlockingFilter.0")); //$NON-NLS-1$
        return true;
    }

    /**
     * Does not permit any records to be emitted from this record processing step.
     *
     * @return Always returns a null Object.
     */
    @Override
    protected final RecordCapsule filterRecord(RecordCapsule o) {
        if (!initialized) {
            initialized = doInitialization();
        }

        logger.debug(AleppoMessages.getString("BlockingFilter.1") + o); //$NON-NLS-1$

        return null;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("BlockingFilter");
}
