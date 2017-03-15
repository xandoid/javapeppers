/**
 *
 */
/**
 *
 */
package com.codeondemand.javapeppers.aleppo.source;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * @author gfa
 */
public interface AbstractConcentrator {
    /**
     * This method is essentially a formatting method that converts the Object
     * that has been accumulated into a format suitable for the output of a
     * RecordSource.
     *
     * @param currentRecord The current record object.
     * @return An object representation of the current record that is
     * appropriate to the application being supported.
     */
    RecordCapsule buildRecord(RecordCapsule currentRecord);

    /**
     * This should return an Object representation of the data Header that is
     * appropriate for the application being supported. Typically this will just
     * be a delimited String, but it could also be a Vector or other type of
     * list.
     *
     * @return An object that represents a header for a data record.
     */
    RecordCapsule buildHeaderObject(RecordCapsule hdrRecord);

    /**
     * Compares two record keys to determine if they are the same. This may just
     * be a very simple string compare, but it is left as an abstract method
     * because it is critical to the concentrators operation.
     *
     * @return true if the keys are the same, otherwise false
     */
    boolean checkKeys(RecordCapsule newkey, RecordCapsule oldkey);

    /**
     * This method should accumulate the data from the new record with the data
     * from the existing record and pass back an Object that represents the
     * combination.
     *
     * @param newrec The new record object.
     * @param oldrec The old record object
     * @return An object that accumulates the new record into the old record.
     */
    RecordCapsule accumulateRecord(RecordCapsule newrec, RecordCapsule oldrec);

}
