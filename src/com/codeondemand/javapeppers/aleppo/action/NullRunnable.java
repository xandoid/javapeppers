/**
 *
 */
package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

/**
 * This class provides a place holder that can be used as a null
 * processing step in a dataflow.  It always sends a notification
 * of success -- notifyObserver(true) -- before it exits the
 * run method.
 * <p>
 * It logs the properties it is passed to the debug stream of
 * the logger.
 *
 * @author gfa
 */
public class NullRunnable extends AleppoRunnable {

    @Override
    public void run() {
        Properties p = null;
        if (processData != null && (p = (Properties) processData.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY)) != null) {
            for (Object o : p.keySet()) {
                String key = (String) o;
                logger.debug(key + ":" + p.getProperty(key));
            }
        }
        setChanged();
        notifyObservers(true);
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("NullRunnable");

}
