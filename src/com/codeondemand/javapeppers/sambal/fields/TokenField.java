package com.codeondemand.javapeppers.sambal.fields;

import com.codeondemand.javapeppers.sambal.util.UtilityGenerator;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

public class TokenField extends DataField {

    public TokenField(String name) {
        this.name = name;
    }

    public boolean isNumeric() {
        boolean retval = false;
        if (type == NUMERIC_TOKEN || type == GAUSSIAN_DBL_TOKEN || type == GAUSSIAN_INT_TOKEN || type == IDENTITY_TOKEN) {
            retval = true;
        }
        return retval;
    }

    public boolean initialize(int type) {
        boolean retval = false;
        if (type >= MIN_TOKEN_TYPE && type <= MAX_TOKEN_TYPE) {
            this.type = type;
            retval = true;
        }
        return retval;
    }

    public boolean initialize(int type, int length) {

        boolean retval = false;
        if (length > 0) {
            this.length = length;
            retval = initialize(type);
        }
        return retval;
    }

    public boolean initialize(int type, int length, int precision) {
        boolean retval = false;
        if (type == NUMERIC_TOKEN || type == GAUSSIAN_DBL_TOKEN || type == GAUSSIAN_INT_TOKEN || type == IDENTITY_TOKEN) {
            if (precision > -1) {
                this.precision = precision;
                retval = initialize(type, length);
            }
        }
        return retval;
    }

    public boolean initialize(int type, int length, int precision, String del, String sep) {
        boolean retval = false;
        if (type == NUMERIC_TOKEN) {
            delimiter = del;
            decsep = sep;
            retval = initialize(type, length, precision);
        }
        return retval;
    }

    @Override
    public String getNextValue() {
        switch (type) {
            case ALPHA_NUMERIC_TOKEN:
                current_value = UtilityGenerator.generateRandomAlphaNumericString(length);
                break;
            case NUMERIC_TOKEN:
                current_value = UtilityGenerator.generateRandomNumericString(length, precision, delimiter, decsep);
                break;
            case NAT_ID_TOKEN:
                current_value = UtilityGenerator.generateNationalIdNumber(country);
                break;
            case LITERAL_TOKEN:
                current_value = literal_val;
                break;
            case SHA256_TOKEN:
                if (mMessageDigest == null) {
                    try {
                        mMessageDigest = MessageDigest.getInstance("SHA-256");
                        current_value = UtilityGenerator.generateSHA256(mMessageDigest);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
                break;
            case IDENTITY_TOKEN:
                identity_val = identity_val + 1;
                if (length > 0) {
                    String temp = "000000000000000" + identity_val;
                    current_value = temp.substring(temp.length() - length);
                } else {
                    current_value = "" + identity_val;
                }
                break;
            case TIMESTAMP_TOKEN:
                if (minDate != null && maxDate != null) {
                    long min = minDate.getTimeInMillis();
                    long max = maxDate.getTimeInMillis();
                    current_value = UtilityGenerator.generateTimeStamp(min, max);
                } else {
                    current_value = UtilityGenerator.generateTimeStamp(new GregorianCalendar().getTimeInMillis());
                }
                break;
            case LIST_TOKEN:
                int index = rangen.nextInt(list.size());
                current_value = list.get(index);
                break;
            case DATE_TOKEN:
                if (minDate != null && maxDate != null) {
                    long min = minDate.getTimeInMillis();
                    long max = maxDate.getTimeInMillis();
                    current_value = UtilityGenerator.generateDate(min, max);
                } else {
                    current_value = UtilityGenerator.generateDate(new GregorianCalendar().getTimeInMillis());
                }
                break;
            case IP_TOKEN:
                current_value = UtilityGenerator.generateIP();
                break;
            default:
                current_value = null;
                break;
        }
        return (String) current_value;
    }

    public boolean setCountry(String c) {
        boolean retval = false;
        if (c.equalsIgnoreCase("us") || c.equalsIgnoreCase("uk")) {
            country = c;
            retval = true;
        }
        return retval;
    }

    public boolean setList(ArrayList<String> l) {
        list = l;
        return true;
    }

    public boolean setIdentityValue(long v) {
        identity_val = v;
        return true;
    }

    public boolean setLiteralValue(String val) {
        literal_val = val;
        return true;
    }

    public boolean setMinDate(int year, int month, int day) {
        minDate = new GregorianCalendar();
        minDate.set(Calendar.YEAR, year);
        minDate.set(Calendar.MONTH, month);
        minDate.set(Calendar.DAY_OF_MONTH, day);
        return true;
    }

    public boolean setMaxDate(int year, int month, int day) {
        maxDate = new GregorianCalendar();
        maxDate.set(Calendar.YEAR, year);
        maxDate.set(Calendar.MONTH, month);
        maxDate.set(Calendar.DAY_OF_MONTH, day);
        return true;
    }

    public String getName() {
        return name;
    }

    protected int type = UNDEFINED_TOKEN;
    private String literal_val = "";
    private long identity_val = 0L;
    private int length = 0;
    private int precision = 0;
    private String delimiter = ",";
    private String decsep = ".";
    private String country = "us";
    private String name = null;
    private GregorianCalendar maxDate = null;
    private GregorianCalendar minDate = null;
    private Random rangen = new Random();
    private ArrayList<String> list = null;
    private static MessageDigest mMessageDigest = null;
    public static final int UNDEFINED_TOKEN = 0;
    public static final int MIN_TOKEN_TYPE = 1;

    public static final int ALPHA_NUMERIC_TOKEN = 1;
    public static final int ALPHA_TOKEN = 2;
    public static final int NUMERIC_TOKEN = 3;
    public static final int NAT_ID_TOKEN = 4;
    public static final int TIMESTAMP_TOKEN = 5;
    public static final int IP_TOKEN = 6;
    public static final int GAUSSIAN_INT_TOKEN = 7;
    public static final int GAUSSIAN_DBL_TOKEN = 8;
    public static final int CATEGORY_TOKEN = 9;
    public static final int DATE_TOKEN = 10;
    public static final int LIST_TOKEN = 11;
    public static final int IDENTITY_TOKEN = 12;
    public static final int LITERAL_TOKEN = 13;
    public static final int SHA256_TOKEN = 14;

    public static final int MAX_TOKEN_TYPE = 14;

}
