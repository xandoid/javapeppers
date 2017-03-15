package com.codeondemand.javapeppers.habanero.util.loader;

import org.apache.logging.log4j.LogManager;

import java.io.*;

public class RecordFile {

    public RecordFile() {
    }

    public RecordFile(String filename) {
        this.filename = filename;
        initialize(filename);
    }

    /**
     * Opens the specified file and reads the first event record to be ready for
     * processing.
     *
     * @param filename
     */
    protected void initialize(String filename) {
        this.filename = filename;
        try {
            rdr = new BufferedReader(new FileReader(new File(filename)));
            if (rdr.ready()) {
                eof = false;
                readNextLine();
            }
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Checks to see if the file has been fully read.
     *
     * @return true if the file is past end of file, otherwise false.
     */
    public boolean isEOF() {
        return eof;
    }

    /**
     * Returns the currently read event string;
     *
     * @return The next event unprocessed event from the file.
     */
    public String getCurrentRecord() {
        String ret = null;
        if (current != null) {
            ret = current;
            readNextLine();
        }
        return ret;
    }

    /**
     * This function needs to be implemented by a subclass to return whaterver
     * portion of the record represents the key. The default behavior if not
     * superceded is to return the whole record.
     */
    public String getRecordKey() {
        return current;
    }

    public String getFilename() {
        return filename;
    }

    public long getCount() {
        return reccount;
    }

    private void readNextLine() {
        try {
            if (!eof) {
                if (rdr.ready()) {
                    current = rdr.readLine();
                    reccount++;
                } else {
                    eof = true;
                    current = null;
                    rdr.close();
                }
            } else {
                current = null;
            }
        } catch (IOException ioe) {
            logger.info("End of file: " + filename);
            eof = true;
        }
    }

    protected String current = null;

    private String filename = null;
    private boolean eof = true;
    private BufferedReader rdr = null;
    private long reccount = 0;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RecordFile");

}
