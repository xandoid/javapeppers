/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.writer;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.logging.log4j.LogManager;

/**
 * The StdDestinationWriter outputs the string representation of the record
 * after trimming whitespace from the end. A newline is written at the end of
 * each record.
 * 
 * @author gfa
 * 
 */
public class StdDestinationWriter extends DestinationWriter {

	public boolean close() {
		try {
			wrtr.flush();
		} catch (IOException e) {
			logger.error(e.toString());
		}
		return true;
	}

	public boolean reset() {
		return close();
	}

	public boolean write(Object record) {
		boolean retval = false;
		try {
			if( record != null ){
				wrtr.write(record.toString().trim() + "\n");
				wrtr.flush();
				retval = true;				
			}
		} catch (IOException e) {
			logger.error(e.toString());
		}
		return retval;
	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("StdDestinationWriter");
	
	private OutputStreamWriter wrtr = new OutputStreamWriter(System.out);

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
}
