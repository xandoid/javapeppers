package com.codeondemand.javapeppers.sambal.util;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;

public class UtilityGenerator {

    /**
     * Generates a 9 digit passport number (no zero characters)
     *
     * @return A 9 digit passport number.
     */
    public static String generatePassportNumber() {

        char[] foo = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
        return sampleCharacters(9, foo);
    }

    /**
     * Generates the string representation of a floating point number of the
     * form xxxx[decimal separator]yyyy
     *
     * @param left   The number of significant digits on the left side of the
     *               decimal separator.
     * @param right  The number of significant digits (without trailing zeroes) on
     *               the right side of the separator.
     * @param decsep The String that should be uses for the decimal separator.
     * @return A String representation of a number.
     */
    public static String generateFloatingPointNumber(int left, int right, String decsep) {
        StringBuilder sb = new StringBuilder();

        if (left > 0) {
            sb.append(generateRandomDigitString(left, false));
        } else {
            sb.append("0");
        }
        if (right > 0) {
            sb.append(decsep + generateRandomDigitString(right, true));
        }
        return sb.toString();
    }

    /**
     * Generates a random string of specified length that contains a mix of
     * alpha characters and digits.
     *
     * @param length The number of characters desired.
     * @return A String with randomly mixed characters and digits.
     */
    public static String generateRandomAlphaNumericString(int length) {
        StringBuilder sbuff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sbuff.append(alphanumeric[rangen.nextInt(alphanumeric.length)]);
        }
        return sbuff.toString().toUpperCase();
    }

    /**
     * Generates a IPv4 compliant string representing an internet address. This
     * is a string of the form ddd.ddd.ddd.ddd where ddd < 256 and the first
     * token does not have leading zeroes
     *
     * @return An IPv4 internet address.
     */
    public static String generateIP(String a, String b, String c, String d) {
        StringBuilder sbuff = new StringBuilder(20);
        if (a != null) {
            sbuff.append(a);
        } else {
            sbuff.append(getIntegerString(100, 255));
        }
        sbuff.append(".");
        if (b != null) {
            sbuff.append(b);
        } else {
            sbuff.append(getIntegerString(0, 255));

        }
        sbuff.append(".");
        if (c != null) {
            sbuff.append(c);
        } else {
            sbuff.append(getIntegerString(0, 255));
        }
        sbuff.append(".");

        if (d != null) {
            sbuff.append(d);
        } else {
            sbuff.append(getIntegerString(0, 255));
        }

        return sbuff.toString();
    }

    /**
     * Generates a IPv4 compliant string representing an internet address. This
     * is a string of the form ddd.ddd.ddd.ddd where ddd < 256 and the first
     * token does not have leading zeroes
     *
     * @return An IPv4 internet address.
     */
    public static String generateIP() {
        return generateIP(null, null, null, null);
    }

    /**
     * Generates a DB2 compatible representation for a timestamp using the
     * specified value.
     *
     * @param value A long integer representing milliseconds after (or before for
     *              negative) January 1, 1970.
     * @return A DB2 compatible string representation of a timestamp.
     */
    public static String generateTimeStamp(long value) {

        java.sql.Date d = new java.sql.Date(value);
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd-hh:mm:ss");
        return sdf.format(d) + "." + generateRandomDigitString(6, true);

    }

    /**
     * Generates a DB2 compatible representation for a date using the specified
     * value.
     *
     * @param min The milliseconds that represent the minimum time for the new
     *            time.
     * @param max The milliseconds that represent the maximum time for the new
     *            time.
     * @return A DB2 compatible string representation of a date
     */
    public static Object generateTimeStamp(long min, long max) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTimeInMillis((long) (min + (rangen.nextDouble() * (max - min))));
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd-hh:mm:ss");
        return sdf.format(new Date(gc.getTimeInMillis())) + generateRandomDigitString(6, true);

    }

    /**
     * Generates a DB2 compatible representation for a date using the specified
     * value.
     *
     * @param value A long integer representing milliseconds after (or before for
     *              negative) January 1, 1970.
     * @return A DB2 compatible string representation of a date
     */
    public static String generateDate(long value) {
        java.sql.Date d = new java.sql.Date(value);
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd");
        return sdf.format(d);

    }

    /**
     * Generates a DB2 compatible representation for a date using the specified
     * value.
     *
     * @param min The milliseconds that represent the minimum time for the new
     *            date.
     * @param max The milliseconds that represent the maximum time for the new
     *            date.
     * @return A DB2 compatible string representation of a date
     */
    public static Object generateDate(long min, long max) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTimeInMillis((long) (min + (rangen.nextDouble() * (max - min))));
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd");
        return sdf.format(new Date(gc.getTimeInMillis()));

    }

    /**
     * Generates a random digit string of specified length, optionally with
     * leading zeroes.
     *
     * @param length           The number of digits.
     * @param hasLeadingZeroes If true, then leading zeroes will be allowed in the output.
     * @return A random string of digits, optionally with leading zeroes.
     */
    public static String generateRandomDigitString(int length, boolean hasLeadingZeroes) {
        StringBuilder sbuff = new StringBuilder(length);

        int firstposadj = 0;
        if (!hasLeadingZeroes) {
            firstposadj = 1;
        }

        for (int i = 0; i < length; i++) {
            sbuff.append(alphanumeric[(26 + firstposadj) + rangen.nextInt(10 - firstposadj)]);
        }

        return sbuff.toString();
    }

    /**
     * This is a partially implemented method that will return an INVALID Social
     * Security Number is the specified country is "us" and a UK health card
     * number that complies with that general format. For all other countries it
     * returns a 10 character random string.
     *
     * @param country Currently supports "us" or "uk" for formatted results
     * @return A US SSN (invalid) or UK health number, or a random 10 char
     * string.
     */
    public static String generateNationalIdNumber(String country) {
        StringBuilder sbuff = new StringBuilder(20);

        if (country == null) {
            sbuff.append(generateRandomAlphaNumericString(10));
        } else if (country.equalsIgnoreCase("us")) {
            // SSNs with a first token between 800 and 999 are not
            // currently in use. Do not allow leading zeroes in th
            // second or third tokens.
            sbuff.append(new Integer(800 + rangen.nextInt(199)));
            sbuff.append("-");
            sbuff.append(generateRandomDigitString(2, false));
            sbuff.append("-");
            sbuff.append(generateRandomDigitString(4, false));
        } else if (country.equalsIgnoreCase("uk")) {

            sbuff.append(sampleCharacters(1, new char[]{'D', 'F', 'I', 'Q', 'U', 'V'}));
            sbuff.append(sampleCharacters(1, alpha).toUpperCase());
            sbuff.append(sampleCharacters(6, new char[]{'1', '2', '3', '4', '5', '6', '7', '8', '9'}));
            sbuff.append(sampleCharacters(1, new char[]{'A', 'B', 'C', 'D'}));

        } else {
            sbuff.append(generateRandomAlphaNumericString(10));
        }
        return sbuff.toString();
    }

    /**
     * Returns a string of specified length sampled from a specified set of
     * characters.
     *
     * @param length  The length of string desired.
     * @param allowed An array of allowable characters.
     * @return A string of the specified length sampled randomly from the
     * allowed characters.
     */
    public static String sampleCharacters(int length, char[] allowed) {
        StringBuilder sbuff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sbuff.append(allowed[rangen.nextInt(allowed.length)]);
        }
        return sbuff.toString();
    }

    /**
     * Generates a numeric string of a specified length and precision,
     * optionally formatted with the thousands delimiter and the decimal
     * specifier supplied.
     *
     * @param length The total length of the string
     * @param prec   The number of digits to the right of the decimal separator
     * @param delim  The string to be used to separate the thousands, etc.
     * @param decsep The String that should be uses for the decimal separator.
     * @return A String representation of a number formatted as requested.
     */
    public static String generateRandomNumericString(int length, int prec, String delim, String decsep) {
        StringBuilder sbuff = new StringBuilder(length);

        // Determine how many digits to skip before putting the first delimiter
        int skip = 0;
        if (length > 3) {
            skip = length % 3;
            sbuff.append(generateRandomDigitString(skip, false));
            // How many three digit groupings left?
            int groups = (length - skip) / 3;

            while (groups > 0) {
                if (sbuff.length() == 0) {
                    sbuff.append(generateRandomDigitString(3, false));
                } else {
                    sbuff.append(delim);
                    sbuff.append(generateRandomDigitString(3, true));
                }
                groups--;
            }
        } else {
            sbuff.append(generateRandomDigitString(length, false));
        }

        if (prec > 0) {
            sbuff.append(decsep);
            sbuff.append(generateRandomDigitString(prec, true));
        }

        return sbuff.toString();
    }

    /**
     * Generates a random string only using english a-z characters.
     *
     * @param length The length of the string desired.
     * @return A string of random characters (a-z only)
     * @see #sampleCharacters
     */
    public static String generateRandomAlphaString(int length) {
        StringBuilder sbuff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sbuff.append(alphanumeric[rangen.nextInt(26)]);
        }
        return sbuff.toString().toUpperCase();
    }

    /**
     * Samples an integer from a gaussian distribution. If this method is called
     * repeatedly, the resultant values should exhibit the statistical
     * characteristics specified in the parameters.
     *
     * @param mean      The mean desired if a series of calls are made to this method.
     * @param stdev     The standard deviation desired for a series of calls made to
     *                  this method.
     * @param invert    Indicates if sampling from an inverted distribution.
     * @param add_noise Adds a bit of noise to a series of samples
     * @param min       Specifies the minimum value desired
     * @param max       Specifies the maximum value desired.
     * @return a sampled integer
     * @see #generateRandomGaussianInteger
     */
    public static int generateRandomGaussianInteger(double mean, double stdev, boolean invert, int add_noise, double min, double max) {
        return new Double(generateRandomGaussian(mean, stdev, invert, add_noise, min, max)).intValue();
    }

    /**
     * Samples an double from a gaussian distribution. If this method is called
     * repeatedly, the resultant values should exhibit the statistical
     * characteristics specified in the parameters.
     *
     * @param mean      The mean desired if a series of calls are made to this method.
     * @param stdev     The standard deviation desired for a series of calls made to
     *                  this method.
     * @param invert    Indicates if sampling from an inverted distribution.
     * @param add_noise Adds a bit of noise to a series of samples
     * @param min       Specifies the minimum value desired
     * @param max       Specifies the maximum value desired.
     * @return A sampled double.
     * @see #generateRandomGaussianInteger
     */
    public static double generateRandomGaussian(double mean, double stdev, boolean invert, int add_noise, double min, double max) {
        if (rangen == null) {
            rangen = new Random();
        }
        if (myGaussian == null) {
            myGaussian = buildGaussianArray(3000, 0, 1);
        }
        if (max > min) {
            double value = max + 1.0;

            while (value > max || value < min) {
                value = mean + (stdev * getRandomElement(myGaussian)) + (2 * stdev * add_noise * ((rangen.nextInt(5) / 10) + 0.5));
            }
            return value;
        } else if (invert) {
            return mean + (stdev * getRandomElement(myGaussian));
        } else {
            return mean + (stdev * getRandomElement(myGaussian)) + (2 * stdev * add_noise * ((rangen.nextInt(5) / 10) + 0.5));
        }
    }

    public static String generateSHA256(MessageDigest md) {

        String retval = null;

        md.update(generateRandomAlphaNumericString(25).getBytes());
        byte[] digest = md.digest();

        Hex h = new Hex();
        retval = new String(h.encode(digest));

        return retval;
    }

    /**
     * Builds a gaussian array for use in some of the other methods.
     *
     * @param count  The number of elements in the array
     * @param mean   The desired mean
     * @param stddev The desired standard deviation of the distribution
     * @return an array of doubles for use in sampling.
     */
    private static double[] buildGaussianArray(int count, double mean, double stddev) {

        if (rangen == null) {
            rangen = new Random();
        }

        double[] foo = new double[count];

        int i = count - 1;
        while (i > 0) {
            double x1 = 0.0;
            double x2 = 0.0;
            double w = 0.0;
            // double y1 = 0.0;
            // double y2 = 0.0;

            do {
                x1 = 2.0 * rangen.nextDouble() - 1.0;
                x2 = 2.0 * rangen.nextDouble() - 1.0;
                w = x1 * x1 + x2 * x2;
            } while (w >= 1.0);

            w = Math.sqrt((-2.0 * Math.log(w)) / w);

            foo[i] = mean + stddev * (x1 * w);
            i--;
            foo[i] = mean + stddev * (x2 * w);
            i--;
        }
        return foo;
    }

    private static String getIntegerString(int min, int max) {
        int i = rangen.nextInt(max - min) + min;
        return new Integer(i).toString();
    }

    private static double getRandomElement(double[] myarray) {
        // TODO Auto-generated method stub
        return myarray[rangen.nextInt(myarray.length)];
    }

    private static char[] alpha = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
    private static char[] alphanumeric = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private static Random rangen = new Random();
    private static double[] myGaussian = buildGaussianArray(3000, 0, 1);

    public static final String PASSPORT_NUMBER = "passport_number";
    public static final String NATIONAL_ID = "national_id";
    public static final String TOKEN_SAMPLE = "token_sample";
    public static final String INTEGER = "integer";
    public static final String TIMESTAMP = "timestamp";
    public static final String DATE = "date";
    public static final String IP_ADDRESS = "ip_address";

    public static final String PARAM_MEAN = "mean";
    public static final String PARAM_MIN = "min";
    public static final String PARAM_MAX = "max";
    public static final String PARAM_STDDEV = "stddev";
    public static final String PARAM_COUNTRY = "country";
    /**
     * @param timeInMillis
     * @param timeInMillis2
     * @return
     */
}
