/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.monitor;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

public abstract class MonitorProcess extends RecordProcessor {

	/**
	 * Allows initialization with data in a RecordCapsule format for
	 * simplicity.
	 * @param rc A RecordCapsule containing information on the fields
	 *           to be monitored.
	 * @return true if successfully initialized.
	 */
	public abstract boolean initialize(RecordCapsule rc );
	
	/**
	 * The implementing class should implement the doMonitor process
	 * in a Thread safe way (of course no code you be non-thread safe)
	 * 
	 * @param input The incoming record.
	 * 
	 * @return true if the record was processed (almost never false) since
	 *              this is really a black hole.
	 */
	protected abstract boolean doMonitor( RecordCapsule input);
	
	/**
	 * Default behavior of the base class is to just increment the record count.
	 * 
	 * @param input   This input is ignored in the base class.
	 * @return true always.
	 */
	public  RecordCapsule processRecord(RecordCapsule input){
		if( doMonitor(input)){
			incrementRecordCount();			
		}
		return input;
	}

	/**
	 * Returns the number of records that this process has
	 * monitored since instantiation or reset.
	 * 
	 * @return The count of monitored records.
	 */
	public long getRecordCount(){
		return recordCount;
	}
	/**
	 * Just increment the record count for this monitor 
	 * class and return the new count.
	 * 
	 * @return The new record count;
	 */
	private synchronized long incrementRecordCount() {
		return ++recordCount;
	}

	/**
	 * Default behavior is to just set the record 
	 * count to zero.
	 * 
	 * @return return true always for the base class.
	 */
	public synchronized boolean reset() {
		recordCount = 0L;
		initialized = false;
		return true;
	}

	
	protected long recordCount = 0L;
	@SuppressWarnings("unused")
	private boolean initialized = false;
}
