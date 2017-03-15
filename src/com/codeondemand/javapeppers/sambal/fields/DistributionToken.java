package com.codeondemand.javapeppers.sambal.fields;

import com.codeondemand.javapeppers.sambal.util.UtilityGenerator;

public class DistributionToken extends TokenField {

    public DistributionToken(String name) {
        super(name);
    }

    public boolean isNumeric() {
        return true;
    }

    public boolean initialize(int type, double mean, double stddev, boolean invert, int add_noise, double min, double max) {

        super.initialize(type);
        this.mean = mean;
        this.stddev = stddev;
        this.min = min;
        this.max = max;
        this.invert = invert;
        this.add_noise = add_noise;
        return true;
    }

    public String getNextValue() {

        switch (type) {
            case TokenField.GAUSSIAN_DBL_TOKEN:
                current_value = new Double(UtilityGenerator.generateRandomGaussian(mean, stddev, invert, add_noise, min, max));
                break;
            case TokenField.GAUSSIAN_INT_TOKEN:
                current_value = new Integer(UtilityGenerator.generateRandomGaussianInteger(mean, stddev, invert, add_noise, min, max));
                break;
            default:
                current_value = null;
        }

        return current_value == null ? null : current_value.toString();
    }

    private double mean = 0.0;
    private double stddev = 0.0;
    private double min = 0.0;
    private double max = 0.0;
    private boolean invert = false;
    private int add_noise = 0;

}
