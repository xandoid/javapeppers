package com.codeondemand.javapeppers.habanero.util.misc;

import org.apache.logging.log4j.LogManager;

import java.util.Properties;

public class BaseApplication {

    public void initialize() {

        // Load the property file and pull in the properties
        String pfile = System.getProperty("app.property.file");

        if (pfile == null) {
            // Load the property file and pull in the properties
            pfile = getClass().getSimpleName() + ".properties";
        }

        // Attempt to load the properties file.  This may fail, but if so
        // just log an error since we may not have have a valid name
        // for a property file.
        if (pfile != null && !pfile.equals("")) {
            properties = MiscUtil.loadXMLPropertiesFile(pfile);
        } else {
            logger.error("No properties file found or loaded: " + pfile);
        }
    }

    /**
     * Provides a convenience method for accessing a property
     * that has been loaded from the main properties file for
     * the application.
     *
     * @param key          The name of the property.
     * @param defaultValue The default to use if the property
     *                     does not exist.
     * @return The value of the property if it exists, otherwise
     * it returns the default value that was passed
     */
    protected String getProperty(String key, String defaultValue) {
        String ret = defaultValue;
        if (properties != null) {
            ret = properties.getProperty(key, defaultValue);
        }
        return ret;
    }

    /**
     * Provides a convenience method for accessing a property
     * that has been loaded from the main properties file for
     * the application.
     *
     * @param key The name of the property.
     * @return The value of the property if it exists, otherwise null;
     */
    protected String getProperty(String key) {
        String ret = null;
        if (properties != null) {
            ret = properties.getProperty(key);
        }
        return ret;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
    }

    protected Properties properties = null;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("BaseApplication");

}
