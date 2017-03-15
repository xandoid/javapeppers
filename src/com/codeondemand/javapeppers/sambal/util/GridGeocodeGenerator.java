/**
 *
 */
package com.codeondemand.javapeppers.sambal.util;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import org.apache.logging.log4j.LogManager;

import java.util.Random;
import java.util.TreeMap;

public class GridGeocodeGenerator extends RecordProcessor {

    @SuppressWarnings("unchecked")
    public boolean initialize(Object[] params) {

        boolean retval = false;

        if (params != null && params.length > 0 && params[0] instanceof TreeMap) {
            TreeMap<String, String> map = (TreeMap<String, String>) params[0];

            if (map.containsKey("min_lat") && map.containsKey("max_lat") && map.containsKey("min_long") && map.containsKey("max_long") && map.containsKey("lat_incr") && map.containsKey("long_incr") && map.containsKey("lat_tag") && map.containsKey("long_tag")) {
                min_lat = new Double(map.get("min_lat")).doubleValue();
                max_lat = new Double(map.get("max_lat")).doubleValue();
                //lat_incr = new Double(map.get("lat_incr")).doubleValue();
                min_long = new Double(map.get("min_long")).doubleValue();
                max_long = new Double(map.get("max_long")).doubleValue();
                //long_incr = new Double(map.get("long_incr")).doubleValue();
                lat_tag = map.get("lat_tag");
                long_tag = map.get("long_tag");
                lat_range = (int) (100000 * (max_lat - min_lat));
                long_range = (int) (100000 * (max_long - min_long));
                retval = true;
            }

        }

        return retval;
    }

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {

        if (!initialized) {
            initialized = initialize(pmap);
        }

        if (initialized) {

            double templat = new Double(rangen.nextInt(lat_range) / 100000.0);
            double templong = new Double(rangen.nextInt(long_range) / 100000.0);
            DataCapsule dclat = new DataCapsule(lat_tag, min_lat + templat);
            dclat.setMetaData("typeName", "REAL");
            dclat.setMetaData("type", java.sql.Types.REAL);
            dclat.setMetaData("length", 10);
            DataCapsule dclong = new DataCapsule(long_tag, min_long + templong);
            dclong.setMetaData("typeName", "REAL");
            dclong.setMetaData("type", java.sql.Types.REAL);
            dclong.setMetaData("length", 10);

            logger.debug("added " + lat_tag + " with value:" + templong);
            logger.debug("added " + long_tag + " with value:" + templat);

            input.addDataCapsule(dclat, false);
            input.addDataCapsule(dclong, false);
        }

        return input;
    }

    private double min_long = 0.0;
    private double max_long = 0.0;
    private double min_lat = 0.0;
    private double max_lat = 0.0;
    private Random rangen = new Random();
    private int lat_range = 0;
    private int long_range = 0;
    private String lat_tag = null;
    private String long_tag = null;
    private boolean initialized = false;

    @Override
    public boolean doInitialization() {
        return false;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("GridGeocodeGenerator");

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }
}
