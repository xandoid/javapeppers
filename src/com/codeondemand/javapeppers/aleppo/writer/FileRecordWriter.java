/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

/**
 * The FileRecordWriter outputs the objects that are presented to it to a
 * specified file, either appending to the existing file, or creating a new
 * file.
 * 
 * @author Gary Anderson
 * 
 */
public class FileRecordWriter extends DestinationWriter {

	public boolean close() {
		boolean retval = false;
		if (initialized && wrtr != null) {
			try {
				wrtr.flush();
				wrtr.close();
				wrtr = null;
				initialized = false;
				retval = true;
			} catch (IOException e) {
				logger.error(e.toString());
			}
		} else {
			if( currentFile != null && currentFile.trim().length() > 0){
				logger.error(currentFile+":"+AleppoMessages.getString("FileRecordDestination.0") ); //$NON-NLS-1$
			}
		}
		return retval;
	}

	public boolean reset() {
		close();
		row_count=0L;
		return initialize(basefile, append);
	}

	public boolean write(Object record) {

		boolean retval = true;
		try {
			if (record != null && record.toString().length() > 0) {
				if (pmap.containsKey("field") && record instanceof RecordCapsule) {
					String pfx = "";
					String path = "";
					if (pmap.containsKey("file_pfx")) {
						pfx = (String) pmap.get("file_pfx");
					}
					if (pmap.containsKey("path")) {
						path = (String) pmap.get("path");
					}
					if (((RecordCapsule) record).checkField(pfx + "output_file_name")) {
						close();
						initialized = initialize(
								path + ((RecordCapsule) record).getField(pfx + "output_file_name").getData().toString(),
								append);
					}
					field = (String) pmap.get("field");

					if (isBinary) {
						Object o = ((RecordCapsule) record).getField(field).getData();
						// Class c = o.getClass();
						if (o instanceof byte[]) {
							fstream.write((byte[]) o);
							fstream.flush();
						}
					} else {
						if (pmap.containsKey("field_delim")) {
							String delim = (String) pmap.get("field_delim");
							StringTokenizer stok = new StringTokenizer(field, delim);
							String fdelim = "";
							while (stok.hasMoreElements()) {
								String tok = stok.nextToken();
								if (((RecordCapsule) record).getField(tok) == null
										|| ((RecordCapsule) record).getField(tok).isNull()) {
									wrtr.write(fdelim + tok + "");
								} else {
									wrtr.write(fdelim
											+ ((RecordCapsule) record).getField(tok).getData().toString().trim());
								}
								fdelim = delim;
							}
							wrtr.write("\n");
						} else {
							String ts = "";
							if( doTS){
								GregorianCalendar gc = new GregorianCalendar();
								SimpleDateFormat sdf = new SimpleDateFormat();
								sdf.applyPattern("yyyyMMdd_HHmmss");
								ts = sdf.format(gc.getTime())+"|";
							}
							if (((RecordCapsule) record).getField(field) != null
									&& !((RecordCapsule) record).getField(field).isNull()) {
								wrtr.write(ts+((RecordCapsule) record).getField(field).getData().toString() + "\n");

							}
						}

						wrtr.flush();
					}
				} else {
					logger.debug("Writing: " + record.toString());
					wrtr.write(record.toString().trim() + "\n");
					wrtr.flush();
				}
				row_count = row_count +1;
				if( row_count == max_rows){
					reset();
				}
			}
		} catch (IOException e) {
			retval = false;
			logger.error(e.toString());
		}
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
				if (pmap.containsKey("isBinary")) {
					isBinary = Boolean.parseBoolean((String) pmap.get("isBinary"));
				}

				
				if (isBinary) {
					fstream = new FileOutputStream(file);
				} else {
					if (pmap.containsKey("doTS")){
						doTS = true;
					}
					wrtr = new BufferedWriter(new FileWriter(file, append));
					currentFile = file.getAbsolutePath();
					if( pmap.containsKey("chmod")){
						Runtime.getRuntime().exec("chmod "+pmap.get("chmod")+" "+currentFile);						
					}
					
					logger.debug(AleppoMessages.getString("FileRecordDestination.5") + currentFile //$NON-NLS-1$
							+ AleppoMessages.getString("FileRecordDestination.6") + append); //$NON-NLS-1$
				}
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
		basefile=new String(filename);
		if( pmap.containsKey("max_rows")){
			max_rows = Long.parseLong((String)pmap.get("max_rows"));
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

			myfilename = MiscUtil.mapString(props, myfilename, "%");
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
		}else{
			retval=true;
		}
		return retval;
	}

	protected BufferedWriter wrtr = null;
	private FileOutputStream fstream = null;
	protected String currentFile = null;
	private boolean append = false;
	// private String delimiter = null;
	private boolean initialized = false;
	private String field = null;
	private boolean isBinary = false;
	private String basefile = null;
	private long max_rows = Long.MAX_VALUE;
	private long row_count = 0L;
	private boolean doTS = false;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FileRecordWriter");

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

}
