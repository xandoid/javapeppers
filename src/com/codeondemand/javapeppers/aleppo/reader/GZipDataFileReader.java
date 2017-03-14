package com.codeondemand.javapeppers.aleppo.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;

public class GZipDataFileReader extends FileSourceReader {

	/**
	 * Allows initialization with a previously opened File.
	 * 
	 * @param file An open file which allows reading.
	 * @return true if the file could be initialized.
	 */
	public boolean initialize(File file) {
		boolean retval = false;

		if (file != null) {
			logger.debug(AleppoMessages.getString("FileSourceReader.4") //$NON-NLS-1$
					+ file.getName());

			try {
				if (file.canRead()) {
					GZIPInputStream gzs = new GZIPInputStream(new FileInputStream(file));
					
					rdr = new BufferedReader(new InputStreamReader( gzs));
					currentFile = file.getAbsolutePath();
					if (rdr.ready()) {
						endOfRecords = false;
						//read();
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

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("GZipDataFileReader");
}
