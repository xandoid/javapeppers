/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.action;

import java.util.Observable;
import java.util.TreeMap;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * An AleppoRunnable is just a normal runnable that has in addition some
 * context information.  The pmap variable will contain the parameters
 * from the section of the XML configuration files where this class was
 * reference and instantiated from, and the processData will be the 
 * Properties that provide the larger context of they Aleppo process 
 * being run.
 * 
 * @author Gary Anderson
 *
 */
public abstract class AleppoRunnable extends Observable implements Runnable {

	/**
	 * Implementing classes should call the notifyObservers method before  
	 * exiting the run method. If the processing was successful, the argument 
	 * passed should be a boolean true, otherwise false. If no value is sent,
	 * then the AleppoProcess class will by default assume that the process 
	 * was not successful.
	 */
	public abstract void run();

	/**
	 * Sets the process data in the form of a RecordCapsule (this should 
	 * contain a Properties object as metadata.
	 * @param data A RecordCapsule containing some properties data.
	 */
	public void setProcessData( RecordCapsule data ){
		processData = data;
	}	
	
	public void setParameters( TreeMap<String,Object>map){
		pmap = map;
	}
	
	protected RecordCapsule processData = null;
	protected TreeMap<String,Object>pmap = null;
}
