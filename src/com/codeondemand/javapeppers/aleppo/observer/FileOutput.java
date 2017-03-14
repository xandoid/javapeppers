/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.observer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Observable;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

/**
 * The FileOutput class outputs the objects that are presented to it to a
 * specified file, either appending to the existing file, or creating a new
 * file.
 * 
 * @author Gary Anderson
 * 
 */
public class FileOutput extends NullObserver {

	public boolean reset() {
		if( wrtr != null){
			try {
				wrtr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		row_count = 0L;
		return initialize(basefile, append);
	}

	public void update(Observable arg0, Object arg1) {
		if( !initialized ){
			initialized = doInitialization();
		}
		try {
			logger.debug("Writing: " + arg1.toString());
			wrtr.write(arg1.toString().trim() + "\n");
			wrtr.flush();
			
			row_count = row_count + 1;
			if (row_count == max_rows) {
				reset();
			}
		} catch (IOException e) {
			logger.error(e.toString());
		}
	}

	public boolean doInitialization(){
		boolean retval = false;
		String file =null;
		if( pmap.containsKey("file")){
			file=(String)pmap.get("file");
		}
		if( pmap.containsKey("append")){
			append=Boolean.parseBoolean((String)pmap.get("append"));
		}
		retval=initialize(file,append);		
		return retval;
	}
	/**
	 * Allows initialization with a previously opened File.
	 * 
	 * @param file
	 *            An open file which allows reading.
	 * @return true if initialized successfully.
	 */
	public boolean initialize(File file, boolean append) {
		boolean retval = false;
		this.append = append;

		if (file != null) {
			try {

				wrtr = new BufferedWriter(new FileWriter(file, append));
				currentFile = file.getAbsolutePath();
				logger.debug(AleppoMessages.getString("FileRecordDestination.5") + currentFile //$NON-NLS-1$
						+ AleppoMessages.getString("FileRecordDestination.6") + append); //$NON-NLS-1$

				retval = true;
			} catch (FileNotFoundException fnfe) {
				logger.error(fnfe.toString());
			} catch (IOException ioe) {
				logger.error(ioe.toString());
			}
		}
		initialized = retval;
		return retval;
	}

	/**
	 * Initializes this record destination with the specified file.
	 * 
	 * <br>
	 * <br>
	 * Note: if the filename contains the following tokens they will be
	 * substituted with values:
	 * <ul>
	 * <li>%TS% - The token will be replaced by a timestamp of the form
	 * yyyy-MM-dd-HH-mmss</li>
	 * <li>%DT% - The token will be replaced with a date yyyy-mm-dd</li>
	 * </ul>
	 * 
	 * @param filename
	 *            The name of the file to write.
	 * @param append
	 *            If true, then then if there is an existing file, the new
	 *            contents will be appended to it, otherwise the file will be
	 *            overwritten.
	 * @return true If the file can be opened and initialized as a record
	 *         source.
	 */
	public boolean initialize(String filename, boolean append) {
		boolean retval = false;
		basefile = new String(filename);
		if (pmap.containsKey("max_rows")) {
			max_rows = Long.parseLong((String) pmap.get("max_rows"));
		}

		if (filename != null && filename.trim().length() > 0) {

			URI uri = null;
			File f = null;

			String myfilename = filename;
			if (filename.contains("%TS%")) {
				GregorianCalendar gc = new GregorianCalendar();
				SimpleDateFormat sdf = new SimpleDateFormat();
				sdf.applyPattern("yyyyMMdd_HHmmss");
				String ts = sdf.format(gc.getTime());
				myfilename = filename.replace("%TS%", ts);
			} else if (filename.contains("%DT%")) {
				GregorianCalendar gc = new GregorianCalendar();
				SimpleDateFormat sdf = new SimpleDateFormat();
				sdf.applyPattern("yyyy-MM-dd");
				String ts = sdf.format(gc.getTime());
				myfilename = filename.replace("%DT%", ts);
			}

			if( props != null){
				myfilename = MiscUtil.mapString(props, myfilename, "%");	
			}
			
			
			try {
				if (myfilename.startsWith("file://")) { //$NON-NLS-1$
					uri = new URI(myfilename);
					f = new File(uri);
				} else {
					f = new File(myfilename);
				}
				if (f != null) {
					retval = initialize(f, append);
				}
			} catch (URISyntaxException e) {
				logger.error(e.toString());
			}
		} else {
			retval = true;
		}
		return retval;
	}

	protected BufferedWriter wrtr = null;
	protected String currentFile = null;
	private boolean append = false;
	// private String delimiter = null;
	private  boolean initialized = false;

	private String basefile = null;
	private long max_rows = Long.MAX_VALUE;
	private long row_count = 0L;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FileOutput");

}
