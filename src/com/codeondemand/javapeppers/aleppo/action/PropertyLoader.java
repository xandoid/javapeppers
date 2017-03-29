package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

/**
 * The PropertyLoader class expects to load a file that is in the standard format
 * for an XML Java property file.
 *
 * @author gfa
 */
public class PropertyLoader extends RecordProcessor {

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {
        if (props != null && input != null) {
            // If there is already a global set of properties, then add/update the properties with
            // the values read from this process node.
            if (input.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY) != null) {
                Properties temp = (Properties) input.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY);
                for (Object o : props.keySet()) {
                    String key = (String) o;
                    temp.put(key, props.get(key));
                }
            } else {
                input.setMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY, props.clone());
                logger.debug("Adding global properties to RecordCapsule");
            }
        }
        return input;
    }

    @Override
    public boolean doInitialization() {
        boolean retval = false;
        if (props == null && pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE)) {
            props = MiscUtil.loadXMLPropertiesFile((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE));

            // This just logs the properties that are being loaded.
            if (props != null) {
                for (Object o : props.keySet()) {
                    String key = (String) o;
                    logger.debug("property " + key + " = " + props.getProperty(key));
                }
                retval = true;
            } else {
                logger.error("Unable to load the property file: " + pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE).toString());
            }
        }

        return retval;
    }

    private Properties props = null;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("PropertyLoader");

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }

}
