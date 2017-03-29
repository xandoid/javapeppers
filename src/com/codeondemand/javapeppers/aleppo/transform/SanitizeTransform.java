/**
 *
 */
package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.sambal.util.UtilityGenerator;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.StringTokenizer;

public class SanitizeTransform extends RecordTransform {

    public boolean doInitialization() {
        boolean retval = false;

        if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD)) {
            field = (String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD);
            retval = true;
        } else {
            retval = false;
            logger.error("Unable to initialize SanitizeTransform: Missing field parameter.");
        }

        if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_TYPE)) {
            type = (String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_TYPE);
            retval = true;
        } else {
            logger.error("Unable to initialize SanitizeTransform: Missing field parameter.");
            retval = false;
        }

        return retval;
    }

    public RecordCapsule doTransform(RecordCapsule input) {

        DataCapsule dc = null;
        if ((dc = input.getField(field)) != null) {
            if (type.equals(UtilityGenerator.PASSPORT_NUMBER)) {
                setPassportNumberData(dc);
            } else if (type.equals(UtilityGenerator.TIMESTAMP)) {
                setTimestampData(dc);
            } else if (type.equals(UtilityGenerator.DATE)) {
                setDateData(dc);
            } else if (type.equals(UtilityGenerator.NATIONAL_ID)) {
                dc.setData(UtilityGenerator.generateNationalIdNumber((String) pmap.get("country")));
            } else if (type.equals(UtilityGenerator.TOKEN_SAMPLE)) {
                setTokenSampleData(dc);
            } else if (type.equals(UtilityGenerator.IP_ADDRESS)) {
                setIPAddressData(dc);
            } else if (type.equals(UtilityGenerator.INTEGER)) {
                setIntegerData(dc);
            }
        }
        return input;
    }


    private static void setPassportNumberData(DataCapsule dc) {
        dc.setData(UtilityGenerator.generatePassportNumber());
    }

    private void setTimestampData(DataCapsule dc) {
        GregorianCalendar min = new GregorianCalendar();
        min.clear();
        min.set(1900, 0, 0);
        GregorianCalendar max = new GregorianCalendar();
        max.clear();
        max.set(2011, 12, 31);

        if (pmap.containsKey(UtilityGenerator.PARAM_MIN)) {
            String tmin = pmap.get(UtilityGenerator.PARAM_MIN).toString();
            StringTokenizer stok = new StringTokenizer(tmin, "-");
            min.set(Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()));
        }
        if (pmap.containsKey(UtilityGenerator.PARAM_MAX)) {
            String tmax = pmap.get(UtilityGenerator.PARAM_MAX).toString();
            StringTokenizer stok = new StringTokenizer(tmax, "-");
            max.set(Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()));
        }
        dc.setData(UtilityGenerator.generateTimeStamp(min.getTimeInMillis(), max.getTimeInMillis()));

    }

    private void setDateData(DataCapsule dc) {
        GregorianCalendar min = new GregorianCalendar();
        min.clear();
        min.set(1900, 0, 0);
        GregorianCalendar max = new GregorianCalendar();
        max.clear();
        max.set(2011, 12, 31);

        if (pmap.containsKey(UtilityGenerator.PARAM_MIN)) {
            String tmin = pmap.get(UtilityGenerator.PARAM_MIN).toString();
            StringTokenizer stok = new StringTokenizer(tmin, "-");
            min.set(Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()));
        }
        if (pmap.containsKey(UtilityGenerator.PARAM_MAX)) {
            String tmax = pmap.get(UtilityGenerator.PARAM_MAX).toString();
            StringTokenizer stok = new StringTokenizer(tmax, "-");
            max.set(Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()), Integer.parseInt(stok.nextToken()));
        }

        dc.setData(UtilityGenerator.generateDate(min.getTimeInMillis(), max.getTimeInMillis()));

    }

    private void setTokenSampleData(DataCapsule dc) {
        if (token_list == null && pmap.containsKey("values")) {
            token_list = new ArrayList<String>();
            StringTokenizer stok = new StringTokenizer(pmap.get("values").toString(), "|");
            while (stok.hasMoreTokens()) {
                token_list.add(stok.nextToken());
            }
        }
        if (token_list != null && token_list.size() > 0) {
            dc.setData(token_list.get(rangen.nextInt(token_list.size())));
        }
    }

    private void setIPAddressData(DataCapsule dc) {
        dc.setData(UtilityGenerator.generateIP((String) pmap.get("ip1"), (String) pmap.get("ip2"), (String) pmap.get("ip3"), (String) pmap.get("ip4")));

    }

    private void setIntegerData(DataCapsule dc) {
        @SuppressWarnings("unused") double min = 0.0;
        double max = 100.0;
        double stddev = 25.0;
        double mean = 50.0;
        if (pmap.containsKey("mean")) {
            mean = Double.parseDouble(pmap.get(UtilityGenerator.PARAM_MEAN).toString());
        }
        if (pmap.containsKey("min")) {
            min = Double.parseDouble(pmap.get(UtilityGenerator.PARAM_MIN).toString());
        }
        if (pmap.containsKey("max")) {
            max = Double.parseDouble(pmap.get(UtilityGenerator.PARAM_MAX).toString());
        }
        if (pmap.containsKey("stddev")) {
            stddev = Double.parseDouble(pmap.get(UtilityGenerator.PARAM_STDDEV).toString());
        }
        dc.setData(UtilityGenerator.generateRandomGaussianInteger(mean, stddev, false, 0, 0, max));

    }

    private String field = null;
    private String type = null;
    private ArrayList<String> token_list = null;
    private Random rangen = new Random();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SanitizeTransform");

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }
}
