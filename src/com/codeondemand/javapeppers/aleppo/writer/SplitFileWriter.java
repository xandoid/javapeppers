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
public class SplitFileWriter extends DestinationWriter {

	public boolean close() {
		boolean retval = false;
		if (initialized && wrtr != null) {
			try {
				for (int i = 0; i < splits; i++) {
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

	public boolean reset() {
		boolean retval = true;
		close();
		for (int i = 0; i < splits; i++) {
			retval = retval && initialize(currentFile[i], append);
		}
		return retval;
	}

	public boolean write(Object record) {

		boolean retval = true;
		try {
			int isplit = 0;
			if (record != null && record.toString().length() > 0) {
				if (((RecordCapsule) record).getField("split") != null) {
					isplit = Integer.parseInt(((RecordCapsule) record)
							.getField("split").getData().toString());
				}
				if (pmap.containsKey("field")
						&& record instanceof RecordCapsule) {
					field = (String) pmap.get("field");
					if (isBinary) {
						Object o = ((RecordCapsule) record).getField(field)
								.getData();
						// Class c = o.getClass();
						if (o instanceof byte[]) {
							fstream[isplit].write((byte[]) o);
							fstream[isplit].flush();
						}
					} else {
						wrtr[isplit].write(((RecordCapsule) record)
								.getField(field).getData().toString()
								+ "\n");
						wrtr[isplit].flush();
					}
				} else if (pmap.containsKey("fields")
						&& pmap.containsKey("delimiter")) {
					StringBuffer foo = new StringBuffer();
					StringTokenizer stok = new StringTokenizer(
							(String) pmap.get("fields"),
							(String) pmap.get("delimiter"));
					while (stok.hasMoreElements()) {
						String tok = stok.nextToken();
						if ((((RecordCapsule) record).getField(tok) != null)) {
							if (((RecordCapsule) record).getField(tok)
									.getData() != null) {
								foo.append(((RecordCapsule) record)
										.getField(tok).getData().toString()
										+ "|");
							}
						} else {
							foo.append("|");
						}
					}
					wrtr[isplit].write(foo.toString() + "\n");
					wrtr[isplit].flush();

				} else {
					logger.debug("Writing: " + record.toString());
					wrtr[isplit].write(record.toString());
					wrtr[isplit].flush();
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
	public boolean initialize(File file, boolean append, int split) {
		boolean retval = false;
		this.append = append;
		if (file != null) {

			try {
				if (pmap.containsKey("isBinary")) {
					isBinary = Boolean.parseBoolean((String) pmap
							.get("isBinary"));
				}

				if (isBinary) {
					fstream[split] = new FileOutputStream(file);
				} else {
					wrtr[split] = new BufferedWriter(new FileWriter(file,
							append));
					currentFile[split] = file.getAbsolutePath();
					logger.debug(AleppoMessages
							.getString("FileRecordDestination.5") + currentFile[split] //$NON-NLS-1$
							+ AleppoMessages
									.getString("FileRecordDestination.6") + append); //$NON-NLS-1$
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
		boolean retval = true;

		URI uri = null;
		File f = null;
		String myfilename = MiscUtil.mapString(props, filename, "%");
		if (myfilename.contains("%TS%")) {
			GregorianCalendar gc = new GregorianCalendar();
			SimpleDateFormat sdf = new SimpleDateFormat();
			sdf.applyPattern("yyyy-MM-dd-HH-mmss");
			String ts = sdf.format(gc.getTime());
			myfilename = myfilename.replace("%TS%", ts);
		} else if (myfilename.contains("%DT")) {
			GregorianCalendar gc = new GregorianCalendar();
			SimpleDateFormat sdf = new SimpleDateFormat();
			sdf.applyPattern("yyyy-MM-dd");
			String ts = sdf.format(gc.getTime());
			myfilename = myfilename.replace("%DT%", ts);
		}
		if (pmap.containsKey("suffix")) {
			suffix = (String) pmap.get("suffix");
		}
		if (pmap.containsKey("splits")) {
			splits = Integer.parseInt((String) pmap.get("splits"));
			System.out.println("Splits: " + splits);
			wrtr = new BufferedWriter[splits];
			fstream = new FileOutputStream[splits];
			currentFile = new String[splits];
		}

		try {
			for (int i = 0; i < splits; i++) {
				String temp = myfilename + i + suffix;
				System.out.println(temp);
				if (myfilename.startsWith("file://")) { //$NON-NLS-1$
					uri = new URI(temp);
					f = new File(uri);
				} else {
					f = new File(temp);
				}
				if (f != null) {

					retval = retval && initialize(f, append, i);
				}
			}
		} catch (URISyntaxException e) {
			logger.error(e.toString());
		}
		initialized = retval;
		return retval;
	}

	protected BufferedWriter[] wrtr = null;
	private FileOutputStream[] fstream = null;
	protected String[] currentFile = null;
	private boolean append = false;
	// private String delimiter = null;
	protected boolean initialized = false;
	private String field = null;
	private boolean isBinary = false;
	protected int splits = 1;
	protected String suffix = "";

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SplitFileWriter");

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

}
