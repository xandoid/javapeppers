package com.codeondemand.javapeppers.habanero.util.loader;

import java.util.LinkedList;
import java.util.Observable;
import java.util.Properties;

import com.codeondemand.javapeppers.habanero.util.process.Process;

public class RecordLoader extends Process {

	//***********************************************************************
	// Constructors
	//***********************************************************************
	public RecordLoader() {
	};

	//***********************************************************************
	// Public methods and data
	//***********************************************************************
	public void initialize(LinkedList<RecordFile> list, Properties p,
			String uid, String pwd) {
		// TO-DO: what is this?
	}

	//***********************************************************************
	// Implementation for Observer interface
	//***********************************************************************

	/**
	 * This update will occur when an underlying objects have changed.
	 *
	 */
	public synchronized void update(Observable o, Object arg) {
		// TO-DO: what is this?
	}

	public void run() {
		// TODO Auto-generated method stub
		
	}

}
