/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.common;


/**
 * The RecordProcessor is an abstraction for any kind of generalized
 * processing of a RecordCapsule.  Implementations might include transforms, 
 * filters, or some other pattern. The only suggested restriction is
 * that there be a one-to-one correspondence between an input object
 * and an output object (the output object is sometimes null in the 
 * case of a RecordFilter.  Processing that involves a one to many or many
 * to one relationships between input and output should be handled 
 * using the functionality in the connector.
 * 
 * @author gfa
 *
 */
public abstract class RecordProcessor extends FlowNode{
	
	
	/**
	 * Allows initialization with data in a RecordCapsule format for
	 * simplicity.
	 * 
	 * @param rc A RecordCapsule containing information on the fields
	 *           to be monitored.
	 * @return true if successfully initialized.
	 */
	public boolean initialize(RecordCapsule rc ){
		initrc = rc;
		return doInitialization();
	}


	
	/**
	 * Implementers of this method should return the input object or
	 * some object that is a result of a conversion of the input object.
	 * In some cases such as a filtering process, the return object may
	 * be null.
	 * 
	 * @param input The input object for processing
	 * 
	 * @return An updated version of the input object, possibly null;
	 */
	public abstract RecordCapsule processRecord( RecordCapsule input);
	

	/** 
	 * The implementing class can override the done() method if it 
	 * needs to do some reporting, flush files, etc.
	 * 
	 */
	public abstract void done();
	
	protected RecordCapsule initrc = null;
	
}
