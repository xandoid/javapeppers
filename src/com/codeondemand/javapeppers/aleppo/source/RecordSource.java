package com.codeondemand.javapeppers.aleppo.source;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.parser.NullRecordParser;
import com.codeondemand.javapeppers.aleppo.parser.RecordParser;
import com.codeondemand.javapeppers.aleppo.reader.SourceReader;
import com.codeondemand.javapeppers.aleppo.reader.StdSourceReader;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

public class RecordSource implements AbstractRecordSource {

    /**
     * A generic method that must be implemented to close the record source and
     * clean up any associated resources.
     *
     * @return true if successfully closed.
     */
    public boolean closeSource() {
        boolean retval = false;
        if (src_reader != null) {
            retval = src_reader.close();
        }
        return retval;
    }

    /**
     * A convenience method to allow getting the current record multiple times.
     * It could be that multiple other classes are sharing this record source
     * and only one of them controls the advancing of the record.
     *
     * @return An Object representing the current record. Should return null if
     * the record source has not yet read a record.
     */
    public RecordCapsule getCurrentRecord() {
        if (currentRecord == null) {
            currentRecord = getNextRecord();
        }
        return currentRecord;
    }

    /**
     * Provided for classes that override this class.
     *
     * @param args Generic argument passing block
     * @return true if initialized.
     */
    public boolean initialize(Object[] args) {
        return true;
    }

    /**
     * A generic initialization method for all types of data sources, since they
     * will each have there own special initialization requirements.
     *
     * @param r The SourceReader instance to use.
     * @param p The Parser instance to use
     * @return true if successfully initialized.
     */
    public boolean initialize(SourceReader r, RecordParser p) {

        if (r != null) {
            logger.debug("Source initialized with reader:" + r.getClass());
            src_reader = r;
        }

        if (p != null) {
            logger.debug("Source initialized with parser:" + p.getClass());
            rec_parser = p;
        }
        return true;
    }

    /**
     * A generic interface for getting the next record, since different
     * implementations will end up returning different types of information
     * constituting a record.
     *
     * @return An Object representing the current record. Should return null if
     * the record source has not yet read a record.
     */
    public RecordCapsule getNextRecord() {
        RecordCapsule retval = null;
        if (src_reader != null) {
            if (reccount < max_reccount) {
                Object temp = src_reader.read();
                if (temp != null) {
                    currentRecord = rec_parser.parseRecord(temp);
                    if (currentRecord == null) {
                        endOfRecords = true;
                        src_reader.close();
                    } else {
                        retval = currentRecord;
                        reccount++;
                    }
                }
            } else {
                currentRecord = null;
            }
        }
        return retval;
    }

    /**
     * A generic interface for getting the header record(s). This will vary by
     * the type of data source.
     *
     * @return An Object that represents the header record.
     */
    public RecordCapsule getHeaderRecord() {
        RecordCapsule retval = null;
        if (headerRecord == null) {
            retval = rec_parser.parseRecord(src_reader.read());
        } else {
            retval = headerRecord;
        }
        return retval;
    }

    /**
     * A generic method for allowing the record source to be reset so it can be
     * reused.
     *
     * @return true if successful;
     */
    public boolean reset() {
        reccount = 0L;
        currentRecord = null;
        setHeaderRecord(null);

        return src_reader.reset();
    }

    private void setHeaderRecord(RecordCapsule r) {
        this.headerRecord = r;
    }

    /**
     * Get the number of records accessed by this RecordSource.
     *
     * @return Returns the number of records that have been accessed by this
     * RecordSource
     */
    public long getRecordCount() {
        return reccount;
    }

    public boolean setMaxRecordCount(long l) {
        max_reccount = l;
        return true;
    }

    // Convenience to tell if the source is finished.
    protected boolean endOfRecords = true;

    // The count of records that have been processed (will start at zero
    // after a reset)
    protected long reccount = 0L;

    private long max_reccount = Long.MAX_VALUE;

    private RecordCapsule currentRecord = null;
    protected RecordCapsule headerRecord = null;

    // Default to a record parser that does nothing
    protected RecordParser rec_parser = new NullRecordParser();

    // Default to a stdin stream source.
    protected SourceReader src_reader = new StdSourceReader();

    public int getRecordKey() {
        if (currentRecord != null) {
            return currentRecord.getKeyHash();
        } else {
            return 0;
        }
    }

    public void setProperties(Properties props) {
        this.props = props;
        logger.debug("Properties set for RecordSource");
    }

    protected Properties props = null;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RecordSource");
}
