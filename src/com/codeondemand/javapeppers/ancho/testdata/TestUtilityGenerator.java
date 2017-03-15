package com.codeondemand.javapeppers.ancho.testdata;

import com.codeondemand.javapeppers.sambal.fields.TokenField;
import com.codeondemand.javapeppers.sambal.util.UtilityGenerator;
import junit.framework.TestCase;


public class TestUtilityGenerator extends TestCase {

    public static void test() {
        // TODO Auto-generated method stub

        TokenField alphaNumericField = new TokenField("anumfield");
        assertEquals("Initializing alphnumeric field", true, alphaNumericField.initialize(TokenField.ALPHA_NUMERIC_TOKEN, 16));
        System.out.println("10 character alpha numeric: " + alphaNumericField.getNextValue());

        TokenField numericField = new TokenField("numfield");
        assertEquals("Initializing numeric field", true, numericField.initialize(TokenField.NUMERIC_TOKEN, 10, 2));
        System.out.println("10 character numeric (US): " + numericField.getNextValue());

        TokenField numericField2 = new TokenField("numfield2");
        assertEquals("Initializing numeric field", true, numericField2.initialize(TokenField.NUMERIC_TOKEN, 10, 2, ".", ","));
        System.out.println("10 character numeric (UK): " + numericField2.getNextValue());

        TokenField natidfield = new TokenField("natidfield");
        assertEquals("Initializing us nat id  field", true, natidfield.initialize(TokenField.NAT_ID_TOKEN));
        assertEquals("Trying to set country to it:", false, natidfield.setCountry("it"));
        assertEquals("Setting country to us:", true, natidfield.setCountry("us"));
        System.out.println("NAT ID (US): " + natidfield.getNextValue());
        assertEquals("Setting country to uk:", true, natidfield.setCountry("uk"));
        System.out.println("NAT ID (UK): " + natidfield.getNextValue());

    }

    public static void test_generateRandomGaussian() {
        double mean = 10.0;
        double stdev = 2;
        boolean invert = false;
        int add_noise = 1;
        double min = 2;
        double max = 20;

        assertNotNull("Generating gaussian distributed numerics:", UtilityGenerator.generateRandomGaussian(mean, stdev, invert, add_noise, min, max));
    }

    public static void test_generateRandomGaussianInteger() {
        double mean = 10.0;
        double stdev = 2;
        boolean invert = false;
        int add_noise = 1;
        double min = 2;
        double max = 20;

        assertNotNull("Generating gaussian distributed integer:", UtilityGenerator.generateRandomGaussianInteger(mean, stdev, invert, add_noise, min, max));
    }

    public static void test_generateRandomAlphaNumeric() {
        assertEquals("Generating a random alphanumeric string", true, UtilityGenerator.generateRandomAlphaNumericString(10).length() == 10);
    }

    public static void test_generateRandomAlpha() {
        assertEquals("Generating a random alphanumeric string", true, UtilityGenerator.generateRandomAlphaString(10).length() == 10);

    }

    public static void test_generateRandomNumeric() {
        int length = 10;
        int prec = 2;
        String delim = ",";
        String decpt = ".";
        assertNotNull("Generating a random numeric string", UtilityGenerator.generateRandomNumericString(length, prec, delim, decpt));
    }

    public static void test_generateNationalID() {
        assertNotNull("Generating national id for us:", UtilityGenerator.generateNationalIdNumber("us"));
        assertNotNull("Generating national id for uk:", UtilityGenerator.generateNationalIdNumber("uk"));
    }

    public static void test_generateTimestamp(long millis) {
        assertNotNull("Generating a timestamp:", UtilityGenerator.generateTimeStamp(millis));
    }

}
