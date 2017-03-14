/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.source;

import java.util.Properties;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * An AbstractRecordSource provides an interface that allows the implementation
 * of a facade pattern that wraps another RecordSource implementation. It abstracts 
 * only the very basic functions of a class that can emit record based data.
 * 
 * @author gfa
 *
 */
public interface AbstractRecordSource {
	
	/** 
	 * A generic method that must be implemented to close the record source and
	 * clean up any associated resources.
	 * 
	 * @return true if successfully closed.
	 */
	public abstract boolean closeSource();
	
	/**
	 * A convenience method to allow getting the current record multiple times.  It could
	 * be that multiple other classes are sharing this record source and only one of them
	 * controls the advancing of the record.
	 * 
	 * @return An Object representing the current record.  Should return null if the record
	 *         source has not yet read a record.
	 */
	public abstract RecordCapsule getCurrentRecord();

	/**
	 * A generic method that must be implemented by a subclass to return application 
	 * specific key(s) for the record.
	 * 
	 * @return An array of objects that are application specific keys.
	 */
	public abstract int getRecordKey();


	/**
	 * A generic interface for getting the next record, since different implementations
	 * will end up returning different types of information constituting a record.
	 * @return An Object representing the current record.  Should return null if the record
	 *         source has not yet read a record.
	 */
	public abstract RecordCapsule getNextRecord();	
	
	/**
	 * A generic interface for getting the header record(s). This will vary by the type
	 * of data source.  
	 * 
	 * @return An Object that represents the header record.
	 * 
	 */
	public abstract RecordCapsule getHeaderRecord();
	
	/**
	 * A generic method for allowing the record source to be reset so it can be 
	 * reused.
	 * 
	 * @return true if successful;
	 * 
	 */
	public abstract boolean reset();
	
	public abstract void setProperties( Properties props);
	
	
}
