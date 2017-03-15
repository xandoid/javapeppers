package com.codeondemand.javapeppers.aleppo.observer;

import java.util.Observable;
import java.util.Observer;

public class SimpleConsoleOutput extends NullObserver implements Observer {


    @Override
    public void update(Observable arg0, Object arg1) {
        // TODO Auto-generated method stub
        if (arg1 != null) {
            System.out.println(arg0.getClass().getSimpleName() + " : " + arg1.toString());
        } else {
            System.out.println(arg0.getClass().getSimpleName() + ": null argument received.");
        }

    }

}
