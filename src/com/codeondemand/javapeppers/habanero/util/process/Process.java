package com.codeondemand.javapeppers.habanero.util.process;

import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;

public abstract class Process extends Observable implements Observer, Runnable {

	// ***********************************************************************
	// Constructors
	// ***********************************************************************
	public Process() {
	};

	
	// ***********************************************************************
	// Public methods and data
	// ***********************************************************************
	public boolean initialize(Properties p, String uid, String pwd) {
		logger.debug("Process.initialize is a nop method, just returning true.");
		return true;
	}

	// ***********************************************************************
	// Public methods and data
	// ***********************************************************************
	public boolean initialize(Properties p) {
		logger.debug("Process.initialize is a nop method, just returning true.");
		return true;
	}

	public boolean initialize( ProcessInfo p){		
		logger.debug("Process.initialize is a nop method, just returning true.");
		return true;
	}
	// ***********************************************************************
	// Implementation for Observer interface
	// ***********************************************************************

/*	*//**
	 * This update will occur when an underlying objects have changed.
	 * 
	 *//*
	public synchronized void update(Observable o, Object arg) {

	}*/


/*	public void run() {
		setChanged();
		logger.debug( "Notifying "+ this.countObservers() + " Observer(s)");
		synchronized(this ){
			logger.debug("Process is a nop class, just notifying and quitting.");
			try {
				Thread.sleep(1500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			notifyObservers();
		}
	}*/
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("Process");
}
