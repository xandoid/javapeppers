/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.common.FlowNode;

/**
 * An abstract class that has the concept of reading records from a variety
 * of different sources such as files, databases, queues, etc.
 * 
 * @author gfa
 *
 */
public abstract class SourceReader extends FlowNode {

	/**
	 * Closes the source reader.  This should leave it in a state does not allow
	 * successful reading of additional records.
	 * 
	 * @return true if the reader was successfully closed, otherwise false;
	 */
	public abstract boolean close();
	
	/**
	 * Resets the reader source and makes it ready for additional reads.
	 * 
	 * @return true if the reader was reset and can be read from, otherwise false.
	 */
	public abstract boolean reset();
	
	/**
	 * Pulls in an object that the reader considers a record.  Note, some readers
	 * will pull in a header record when the first call is made.  This is 
	 * information that should be known to the application developer you provides
	 * the source being read from.
	 * 
	 * @return An Object that represents a logical record.
	 */
	public abstract Object read();


}
