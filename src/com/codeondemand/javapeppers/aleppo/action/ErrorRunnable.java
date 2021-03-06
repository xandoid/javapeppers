/**
 *
 */
package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

/**
 * This class will fail with a null pointer exception to allow testing
 * of any frameworks that handle things like that.
 * <p>
 * It logs the properties it is passed to the debug stream of
 * the logger.
 *
 * @author gfa
 */
public class ErrorRunnable extends AleppoRunnable {

    @Override
    public void run() {
        boolean result_value = true;
        Properties p = null;
        if (processData != null && (p = (Properties) processData.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY)) != null) {
            for (Object o : p.keySet()) {
                String key = (String) o;
                logger.debug(key + ":" + p.getProperty(key));
            }
        }

        // Create a null pointer exception for testing the control harness if there is one.
        String foo = null;
        @SuppressWarnings({"null", "unused"}) int bar = foo.length();

        setChanged();
        notifyObservers(result_value);
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ErrorRunnable");
}
