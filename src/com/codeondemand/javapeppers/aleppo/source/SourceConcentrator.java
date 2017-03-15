package com.codeondemand.javapeppers.aleppo.source;


import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

/**
 * This class wraps a RecordSource and aggregates information based on whatever
 * the unique key is for the record source. It expects that the underlying
 * source will provide the records ordered by key. For each unique key this
 * class receives, it emits one record. This is useful for rollup types of
 * operations on record sources that do not have built in capabilities for that
 * behavior.
 *
 * @author gfa
 */
public abstract class SourceConcentrator implements AbstractRecordSource, AbstractConcentrator {

    public boolean setSource(RecordSource src) {
        this.src = src;
        return true;
    }

    /**
     * This method returns the current state of the record, getting the first
     * record if it has not yet been retrieved.
     *
     * @return An Object that represents the current record.
     */
    public RecordCapsule getCurrentRecord() {

        if (currentRecord == null) {
            currentRecord = getNextRecord();
        }
        return buildRecord(currentRecord);
    }

    /**
     * The concentrator is likely to have a very different header output than
     * the source input. The implementing class must create a string appropriate
     * to the record it will be creating.
     *
     * @return An Object representing the
     */
    public RecordCapsule getHeaderRecord() {
        return buildHeaderObject(src.getHeaderRecord());
    }

    /**
     * This is the record accumulator logic and should not be overridden by the
     * implementing class. (I guess it won't be given its final nature).
     *
     * @return An Object that represents the next record.
     */
    public RecordCapsule getNextRecord() {
        RecordCapsule retval = null;
        boolean finished = false;
        RecordCapsule nextRecord = null;

        // Initialize the next record if the current record is null
        currentRecord = currentRecord == null ? src.getNextRecord() : currentRecord;
        if (currentRecord != null) {
            accumulateRecord(currentRecord, null);
        } else {
            finished = true;
        }
        while (!finished) {
            if ((nextRecord = src.getNextRecord()) != null && checkKeys(nextRecord, currentRecord)) {
                accumulateRecord(nextRecord, null);
                currentRecord = nextRecord;
            } else {
                retval = buildRecord(currentRecord);
                currentRecord = nextRecord;
                finished = true;
            }
        }

        logger.debug("Returning record: " + retval);
        return retval;
    }

    /**
     * Returns the key of the current record.
     *
     * @return An object that represents the current record key.
     */
    public int getRecordKey() {
        if (currentRecord == null) {
            currentRecord = getNextRecord();
        }
        return src.getRecordKey();
    }

    /**
     * Just pass the close on to the underlying source.
     */
    public boolean closeSource() {
        currentKey = 0;
        return (src != null ? src.closeSource() : false);
    }

    /**
     * Just passes this reset request to the underlying RecordSource if it has
     * been set.
     */
    public boolean reset() {
        currentKey = 0;
        return (src != null ? src.reset() : false);
    }

    /**
     * This method is essentially a formatting method that converts the Object
     * that has been accumulated into a format suitable for the output of a
     * RecordSource.
     *
     * @param currentRecord The current record object.
     * @return An object representation of the current record that is
     * appropriate to the application being supported.
     */
    public abstract RecordCapsule buildRecord(RecordCapsule currentRecord);

    /**
     * This should return an Object representation of the data Header that is
     * appropriate for the application being supported. Typically this will just
     * be a delimited String, but it could also be a Vector or other type of
     * list.
     *
     * @return An object that represents a header for a data record.
     */
    public abstract RecordCapsule buildHeaderObject(RecordCapsule headerRecord);

    /**
     * Compares two record keys to determine if they are the same. This may just
     * be a very simple string compare, but it is left as an abstract method
     * because it is critical to the concentrators operation.
     *
     * @return true if the keys are the same, otherwise false
     */
    public boolean checkKeys(RecordCapsule newkey, RecordCapsule oldkey) {

        boolean retval = false;
        if (newkey != null && oldkey != null) {
            retval = newkey.getKeyString().equals(oldkey.getKeyString());
        }
        return retval;
    }

    /**
     * This method should accumulate the data from the new record with the data
     * from the existing record and pass back an Object that represents the
     * combination.
     *
     * @param newrec The new record object.
     * @param oldrec The old record object
     * @return An object that accumulates the new record into the old record.
     */
    public abstract RecordCapsule accumulateRecord(RecordCapsule newrec, RecordCapsule oldrec);

    // The underlying data source.
    protected RecordSource src = null;

    // The current key
    @SuppressWarnings("unused")
    private int currentKey = 0;

    // The current record
    private RecordCapsule currentRecord = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SourceConcentrator");
}
