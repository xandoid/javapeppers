package com.codeondemand.javapeppers.aleppo.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;

public class GZIPDataFileWriter extends SplitFileWriter {
	/**
	 * Allows initialization with a previously opened File.
	 * 
	 * @param file An open file which allows reading.
	 * @param append - Ignored in this type of case
	 * @param split The index to use for the split
	 * @return true if the file could be initialized.
	 */
	public boolean initialize(File file,boolean append, int split) {
		boolean retval = false;

		if( gzs == null ){
			gzs = new GZIPOutputStream[splits];
		}
		if (file != null) {
			logger.debug(AleppoMessages.getString("FileSourceReader.4") //$NON-NLS-1$
					+ file.getName());

			try {

					gzs[split] = new GZIPOutputStream(new FileOutputStream(file));
					
					wrtr[split] = new BufferedWriter(new OutputStreamWriter( gzs[split]));
					currentFile[split] = file.getAbsolutePath();
					//System.out.println( wrtr[split]);
					if (wrtr[split] != null) {
						retval = true;
					}
					logger.debug(AleppoMessages.getString("FileSourceReader.5")); //$NON-NLS-1$

			} catch (FileNotFoundException fnfe) {
				logger.error(fnfe.toString());
			} catch (IOException ioe) {
				logger.error(ioe.toString());
			}
		}
		initialized = retval;
		return retval;
	}
	
	public boolean close() {
		boolean retval = false;
		if (initialized && wrtr != null) {
			try {
				for (int i = 0; i < splits; i++) {
					gzs[i].finish();
					wrtr[i].flush();
					wrtr[i].close();
				}
				wrtr = null;
				initialized = false;
				retval = true;
			} catch (IOException e) {
				logger.error(e.toString());
			}
		} else {
			logger.error(AleppoMessages.getString("FileRecordDestination.0")); //$NON-NLS-1$
		}
		return retval;
	}
	
	protected GZIPOutputStream gzs[] = null;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("GZIPDataFileWriter");

}
