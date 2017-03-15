package com.codeondemand.javapeppers.sambal.fields;

import java.util.Random;
import java.util.StringTokenizer;

public class CategoryToken extends TokenField {

    public CategoryToken(String name) {
        super(name);
    }

    public String getNextValue() {
        String retval = null;
        if (tlist != null) {
            int idx = rangen.nextInt(tlist.length);
            retval = tlist[idx];
        }
        return retval;
    }

    public boolean initialize(int type, String[] list) {

        super.initialize(type);

        int totalcount = 0;
        for (String aList1 : list) {
            StringTokenizer foo = new StringTokenizer(aList1, ":");
            if (foo.countTokens() == 2) {
                try {
                    // String token =
                    foo.nextToken();
                    int value = new Integer(foo.nextToken());
                    totalcount += value;
                } catch (NumberFormatException nfe) {
                    nfe.printStackTrace();
                }
            }
        }

        double factor = 1.0;
        while (totalcount > 1000) {
            totalcount = totalcount / 10;
            factor = factor / 10;
        }
        tlist = new String[totalcount];
        int index = 0;
        for (String aList : list) {
            StringTokenizer foo = new StringTokenizer(aList, ":");
            if (foo.countTokens() == 2) {
                try {
                    String token = foo.nextToken();
                    int value = new Integer(foo.nextToken());
                    value = (int) (value * factor);
                    for (int j = 0; j < value; j++) {
                        tlist[index++] = token;
                    }
                } catch (NumberFormatException nfe) {
                    nfe.printStackTrace();
                }
            }
        }

        return true;
    }

    private Random rangen = new Random();
    private String[] tlist = null;
}
