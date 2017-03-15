package com.codeondemand.javapeppers.sambal.domain.telco;

import com.codeondemand.javapeppers.aleppo.reader.SourceReader;
import com.codeondemand.javapeppers.sambal.util.UtilityGenerator;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.StringTokenizer;

public class CallDetailRecordReader extends SourceReader {

    @Override
    /**
     * This is a NO OP method for this class that just returns
     * true;
     */ public boolean close() {
        return true;
    }

    @Override
    /**
     * This is a NO OP method for this class that just returns
     * true;
     */ public boolean reset() {
        return true;
    }

    @Override
    public Object read() {
        String retval = null;
        retval = getPhoneNumber() + delimiter + getPhoneNumber();
        retval = retval + delimiter + getCallType();
        retval = retval + delimiter + UtilityGenerator.generateRandomGaussianInteger(11.0, 4.0, false, 0, 0.0, 100.0);
        retval = retval + delimiter + UtilityGenerator.generateTimeStamp(new GregorianCalendar().getTimeInMillis());
        return retval;
    }


    @Override
    public boolean doInitialization() {
        boolean retval = true;

        if (pmap.containsKey("country_codes")) {
            StringTokenizer temp = new StringTokenizer((String) pmap.get("country_codes"), "|");
            while (temp.hasMoreTokens()) {
                country_codes.add(temp.nextToken());
            }
        } else {
            country_codes.add("");
        }

        if (pmap.containsKey("area_codes")) {
            StringTokenizer temp = new StringTokenizer((String) pmap.get("area_codes"), "|");
            while (temp.hasMoreTokens()) {
                area_codes.add(temp.nextToken());
            }
        } else {
            area_codes.add("");
        }

        if (pmap.containsKey("prefix_codes")) {
            StringTokenizer temp = new StringTokenizer((String) pmap.get("prefix_codes"), "|");
            while (temp.hasMoreTokens()) {
                prefix_codes.add(temp.nextToken());
            }
        } else {
            prefix_codes.add("");
        }
        if (pmap.containsKey("type_codes")) {
            StringTokenizer temp = new StringTokenizer((String) pmap.get("type_codes"), "|");
            while (temp.hasMoreTokens()) {
                type_codes.add(temp.nextToken());
            }
        } else {
            type_codes.add("1");
        }

        if (pmap.containsKey("number_length")) {
            number_length = Integer.parseInt((String) pmap.get("number_length"));
        }


        return retval;
    }

    private String getCallType() {
        String retval = type_codes.get(rangen.nextInt(type_codes.size()));
        return retval;
    }

    private String getPhoneNumber() {
        String retval = null;

        retval = "" + country_codes.get(rangen.nextInt(country_codes.size())) + area_codes.get(rangen.nextInt(area_codes.size())) + prefix_codes.get(rangen.nextInt(prefix_codes.size())) + UtilityGenerator.generateRandomDigitString(number_length, true);
        return retval;
    }

    private ArrayList<String> country_codes = new ArrayList<String>();
    private ArrayList<String> area_codes = new ArrayList<String>();
    private ArrayList<String> prefix_codes = new ArrayList<String>();
    private ArrayList<String> type_codes = new ArrayList<String>();
    private int number_length = 10;
    private Random rangen = new Random();
    private String delimiter = "|";
    //private long time_seed = Long.MAX_VALUE;

}
