/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;

/**
 * This class implements SourceReader in the context of reading data from a
 * sequential file.
 * 
 * @author gfa
 *
 */
public class FileSourceReader extends SourceReader {

	@Override
	public boolean doInitialization() {
		boolean retval = false;
		if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE)) {
			String filename = (String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE);
			retval = initialize(filename);
		}
		return retval;
	}

	public boolean close() {
		boolean retval = false;

		if (!closed && rdr != null) {
			try {
				rdr.close();
				rdr = null;
				endOfRecords = true;
				retval = true;
				logger.debug(this.toString() + AleppoMessages.getString("FileSourceReader.9")); //$NON-NLS-1$
			} catch (IOException e) {
				logger.error(e.toString());
			}
			closed = true;
		} else {
			if (!closed) {
				logger.error(AleppoMessages.getString("FileSourceReader.0")); //$NON-NLS-1$
			}
		}
		return retval;
	}

	/**
	 * Initializes this source reader with the specified file.
	 * 
	 * @param filename
	 * @return true If the file can be opened and initialized as a record
	 *         source.
	 */
	public boolean initialize(String filename) {
		boolean retval = false;

		logger.debug(AleppoMessages.getString("FileSourceReader.1")); //$NON-NLS-1$
		File f = null;
		URI uri = null;
		if (filename.startsWith("file://")) { //$NON-NLS-1$
			try {
				uri = new URI(filename);
				if (uri != null){
					f = new File(uri);					
				}
			} catch (URISyntaxException e) {
				logger.error(e.toString());
			}
		} else {
			f = new File(filename);
		}
		if (f != null && f.isFile()) {
			retval = initialize(f);
		} else {
			logger.error(f + AleppoMessages.getString("FileSourceReader.3")); //$NON-NLS-1$
		}
		return retval;
	}

	/**
	 * Allows initialization with a previously opened File.
	 * 
	 * @param file
	 *            An open file which allows reading.
	 * @return true if the file could be initialized.
	 */
	public boolean initialize(File file) {
		boolean retval = false;

		if (file != null) {
			logger.debug(AleppoMessages.getString("FileSourceReader.4") //$NON-NLS-1$
					+ file.getName());

			try {
				if (file.canRead()) {
					rdr = new BufferedReader(new FileReader(file));
					currentFile = file.getAbsolutePath();
					if (rdr.ready()) {
						endOfRecords = false;
						// read();
						retval = true;
					}
					logger.debug(AleppoMessages.getString("FileSourceReader.5")); //$NON-NLS-1$
				} else {
					logger.error(AleppoMessages.getString("FileSourceReader.6") + file.getAbsolutePath()); //$NON-NLS-1$
				}
			} catch (FileNotFoundException fnfe) {
				logger.error(fnfe.toString());
			} catch (IOException ioe) {
				logger.error(ioe.toString());
			}
		}
		return retval;
	}

	public boolean reset() {
		boolean retval = false;
		close();
		retval = initialize(currentFile);
		return retval;
	}

	public Object read() {
		Object retval = null;
		try {
			boolean blank = true;
			if (!endOfRecords) {
				while (blank && rdr.ready()) {

					retval = rdr.readLine();
					logger.debug("record read: " + retval);
					
					// Skip over blank records.
					if( retval != null){
						blank = ((String) retval).trim().length() == 0;						
					}

					// See if that was the last record.
					if (!rdr.ready()) {
						logger.debug(AleppoMessages.getString("FileSourceReader.7") + currentFile); //$NON-NLS-1$
						endOfRecords = true;
					}
				}
			}
		} catch (java.io.EOFException eofe) {
			logger.error(AleppoMessages.getString("FileSourceReader.8")); //$NON-NLS-1$
			endOfRecords = true;
		} catch (IOException ioe) {
			logger.error(ioe.toString());
		}
		return retval;
	}

	protected BufferedReader rdr = null;
	protected String currentFile = null;
	protected boolean endOfRecords = true;
	protected boolean closed = false;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FileSourceReader");

}
