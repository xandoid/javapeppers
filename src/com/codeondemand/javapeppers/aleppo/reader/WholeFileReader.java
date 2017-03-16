/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import org.apache.logging.log4j.LogManager;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class implements SourceReader in the context of reading data from a
 * file and passing the entire contents of the file at once.  The file can
 * be read as binary or text, but the proper property needs to be set in the
 * configuration file.
 *
 * @author gfa
 */
public class WholeFileReader extends SourceReader {

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

        if (rdr != null) {
            try {
                rdr.close();
                rdr = null;
                endOfRecords = true;
                retval = true;
                logger.debug(this.toString() + AleppoMessages.getString("FileSourceReader.9")); //$NON-NLS-1$
            } catch (IOException e) {
                logger.error(e.toString());
            }
        } else {
            logger.error(AleppoMessages.getString("FileSourceReader.0")); //$NON-NLS-1$
        }
        return retval;
    }

    /**
     * Initializes this source reader with the specified file.
     *
     * @param filename The name of the file to for reading.
     * @return true If the file can be opened and initialized as a record
     * source.
     */
    public boolean initialize(String filename) {
        boolean retval = false;

        if (pmap.containsKey("isBinary")) {
            isBinary = Boolean.parseBoolean((String) pmap.get("isBinary"));
        }
        if (pmap.containsKey("doTrim")) {
            doTrim = Boolean.parseBoolean((String) pmap.get("doTrim"));
        }

        logger.debug(AleppoMessages.getString("FileSourceReader.1")); //$NON-NLS-1$
        File f = null;
        URI uri = null;
        if (filename.startsWith("file://")) { //$NON-NLS-1$
            try {
                uri = new URI(filename);
            } catch (URISyntaxException e) {
                logger.error(e.toString());
            }
            if (uri != null) {
                f = new File(uri);
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
                    if (isBinary) {
                        fsize = file.length();
                        if (fsize > MAX_SIZE) {
                            logger.error("File too big for processing");
                        } else {
                            fstream = new FileInputStream(file);
                            if (fstream.available() > 0) {
                                endOfRecords = false;
                            }
                        }
                    } else {
                        rdr = new BufferedReader(new FileReader(file));
                        currentFile = file.getAbsolutePath();
                        if (rdr.ready()) {
                            endOfRecords = false;
                            // read();
                            retval = true;
                        }
                        logger.debug(AleppoMessages.getString("FileSourceReader.5")); //$NON-NLS-1$
                    }
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
        StringBuffer bigbuff = new StringBuffer();
        try {
            if (!endOfRecords) {
                if (isBinary) {
                    byte[] buff = new byte[(int) fsize];
                    int avail = 0;
                    int start = 0;
                    while ((avail = fstream.available()) > 0) {
                        int readin = fstream.read(buff, start, avail);
                        start += readin;
                    }
                    retval = buff;
                } else {
                    while (rdr.ready()) {

                        String temp = rdr.readLine();
                        logger.debug("record read: " + retval);

                        // Skip over blank records.
                        if (temp != null) {
                            if (doTrim) {
                                bigbuff.append((String) temp.toString().trim() + "\n");
                            } else {
                                bigbuff.append((String) temp.toString() + "\n");
                            }

                        }
                        // See if that was the last record.n
                        if (!rdr.ready()) {
                            logger.debug(AleppoMessages.getString("FileSourceReader.7") + currentFile); //$NON-NLS-1$
                            endOfRecords = true;
                            retval = bigbuff.toString();
                        }
                    }
                }
            }
        } catch (java.io.EOFException eofe) {
            logger.error(AleppoMessages.getString("FileSourceReader.8")); //$NON-NLS-1$
            endOfRecords = true;
            retval = bigbuff.toString();
        } catch (IOException ioe) {
            logger.error(ioe.toString());
        }
        return retval;
    }

    private BufferedReader rdr = null;
    private FileInputStream fstream = null;
    private String currentFile = null;
    private boolean endOfRecords = true;
    private boolean isBinary = false;
    private static final long MAX_SIZE = Integer.MAX_VALUE;
    private long fsize = 0L;
    private boolean doTrim = true;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("WholeFileReader");

}
