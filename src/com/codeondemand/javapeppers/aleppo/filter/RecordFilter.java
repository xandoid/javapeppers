package com.codeondemand.javapeppers.aleppo.filter;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

/**
 * This class represents the ability to filter a record set between the source
 * and destination. Classes that implement the functionality should understand
 * the Objects that are being passed through the filter, but should change the
 * object in any manner (use a RecordTransform component for that purpose).
 * 
 * @author gfa
 * 
 */
public abstract class RecordFilter extends RecordProcessor {

	/**
	 * The implementing class should either return the input or a null. Any
	 * implementation of this function should AVOID any modifications to the 
	 * object being filtered.
	 * 
	 * @param input
	 *            The input object to test for processing
	 * 
	 * @return A null if the incoming object is filtered out, otherwise the
	 *         incoming object is returned.
	 */
	protected abstract RecordCapsule filterRecord(RecordCapsule input);

	/**
	 * Base class function that tracks the filter counts after invoking the
	 * 'filterRecord' function implemented by the implementing filter class.
	 * 
	 * @return This should return either a null, or the original input if the
	 *         implementing class does as it should.
	 */
	public RecordCapsule processRecord(RecordCapsule input) {
		RecordCapsule retval = filterRecord(input);
		if (retval == null) {
			incrementObjectsNotPassed();
		} else {
			incrementObjectsPassed();
		}
		return retval;
	}

	/**
	 * Returns the number of objects that were passed through the filter.
	 * 
	 * @return The number of objects that passed through (were not thrown in the
	 *         bit bucket) the filter.
	 */
	public long getObjectsPassed() {
		return objectsPassed;
	}

	/**
	 * Returns the number of objects that were not passed through the filter.
	 * 
	 * @return The number of objects that not passed through (were thrown in the
	 *         bit bucket) the filter.
	 */
	public long getObjectsNotPassed() {
		return objectsNotPassed;
	}

	/**
	 * Resets the values of objectsPassed and objectsNotPassed to 0.
	 * 
	 * @return Always returns true
	 */
	public synchronized boolean reset() {
		objectsPassed = 0L;
		objectsNotPassed = 0L;
		return true;
	}

	private synchronized long incrementObjectsPassed() {
		return objectsPassed++;
	}

	private synchronized long incrementObjectsNotPassed() {
		return objectsNotPassed++;
	}

	protected long objectsNotPassed = 0L;

	protected long objectsPassed = 0L;

	protected boolean initialized = false;

}
