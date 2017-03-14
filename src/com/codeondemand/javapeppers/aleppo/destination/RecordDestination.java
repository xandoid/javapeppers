package com.codeondemand.javapeppers.aleppo.destination;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.builder.RecordBuilder;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.writer.DestinationWriter;

public class RecordDestination {

	/**
	 * Initialize the writer and builder components of a Destination.
	 * 
	 * @param w
	 *            Specifies the DestinationWriter component for this object. If
	 *            you pass a 'null' for the parameter, then a
	 *            com.codeondemand.javapeppers.aleppo.writer.NullWriter object will be instantiated
	 *            and used for the writer.
	 * @param b
	 *            Specifies the DestinationWriter component for this object. If
	 *            you pass a 'null' for the parameter, then a
	 *            com.codeondemand.javapeppers.aleppo.builder.NullBuilder object will be instantiated
	 *            and used for the writer.
	 * @return true if there were no errors encountered.
	 */
	public boolean initialize(DestinationWriter w, RecordBuilder b) {

		boolean retval = true;

		// Set the writer
		if (w == null) {
			retval = retval
					&& setDestinationWriter(new com.codeondemand.javapeppers.aleppo.writer.NullWriter());
			logger.debug("null object passed for writer, "
					+ "instantiating a com.codeondemand.javapeppers.aleppo.writer.NullWriter object.");
		} else {
			retval = retval && setDestinationWriter(w);
			logger.debug("Initializing destination with writer: "
					+ w.toString());
		}
		// Set the builder
		if (b == null) {
			retval = retval
					&& setRecordBuilder(new com.codeondemand.javapeppers.aleppo.builder.NullBuilder());
			logger.debug("null object passed for builder, "
					+ "instantiating a com.codeondemand.javapeppers.aleppo.builder.NullBuilder object.");
		} else {
			retval = retval && setRecordBuilder(b);
			logger.debug("Initializing destination with writer: "
					+ w.toString());
		}

		return retval;
	}

	/**
	 * This method resets the writer component of the destination and resets the
	 * record counter to zero.
	 * 
	 * @return true if the reset was successful.
	 */
	public boolean resetDestination() {
		boolean retval = false;

		if (dest_writer != null) {
			retval = dest_writer.reset();
			reccount = 0L;
		}

		return retval;
	}

	/**
	 * Closes the writer component. For instance, in the case of a file writer,
	 * it is expected that the file will be closed.
	 * 
	 * @return true if the writer is closed successfully.
	 */
	public boolean closeDestination() {
		boolean retval = false;
		if (dest_writer != null) {
			retval = dest_writer.close();
		}
		return retval;
	}

	/**
	 * 
	 * @param record The incoming payload
	 * @return true if the header is processed
	 */
	public boolean setHeaderRecord(RecordCapsule record) {
		boolean retval = false;
		if (builder != null) {
			Object temp = builder.buildHeader(record);
			retval = doOutput(temp);
		}
		return retval;
	}

	/**
	 * Returns the number of records that have been processed to this
	 * destination since initialization or the last reset.
	 * 
	 * @return The number of records processed.
	 */
	public synchronized long getRecordCount() {
		return reccount;
	}

	/**
	 * Requests the the incoming RecordCapsule be converted to an output format
	 * and written to the destination target.
	 * 
	 * @param record
	 *            The incoming RecordCapsule object.
	 * 
	 * @return true if the record was successfully written or if the maximum
	 *         number of records have already been output.
	 */
	public boolean setRecord(RecordCapsule record) {
		boolean retval = false;

		// Limit the output to the maximum, but return true if the limit is
		// exceeded. (Is this the correct logic?)
		if (reccount < maxreccount) {
			Object temp = null;
			// OK to proceed if for some reason the builder is null
			if (builder != null ) {
				temp = builder.buildRecord(record);
			}
			if (dest_writer != null) {
				retval = dest_writer.write(temp);
				if (retval) {
					reccount++;
				}
			}
		} else {
			retval = true;
		}
		return retval;
	}

	/**
	 * Sets the maximum number of records that may be output to the destination
	 * target. This defaults to Long.MAXVALUE if this method is not called.
	 * 
	 * @param max
	 *            The number of records you can send.
	 */
	public void setMaxRecordCount(long max) {
		if (max > 0) {
			maxreccount = max;
		}
	}

	/**
	 * Returns the maximum number of records that are allowed to be output to
	 * the destination target.
	 * 
	 * @return The maximum number of output records.
	 */
	public long getMaxRecordCount() {
		return maxreccount;
	}
	
	public void setProperties( Properties p){
		if( dest_writer != null){
			dest_writer.setProperties(p);
		}
		if( builder != null){
			builder.setProperties(p);
		}
	}

	// Specifies the destination writer.
	protected boolean setDestinationWriter(DestinationWriter w) {
		dest_writer = w;
		w.activate();
		return true;
	}

	protected boolean setRecordBuilder(RecordBuilder bldr) {
		builder = bldr;
		return true;
	}

	// Request output to the target destination. (Is it ok to allow
	// a null output item?  I think it probably is)
	private boolean doOutput(Object output) {
		boolean retval = false;
		if (dest_writer != null ) {
			retval = dest_writer.write(output);
		}
		return retval;
	}

	protected DestinationWriter dest_writer = null;
	protected RecordBuilder builder = null;
	protected long reccount = 0L;
	protected long maxreccount = Long.MAX_VALUE;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RecordDestination");
}
