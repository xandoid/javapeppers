/**
 *
 */
package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

/**
 * The RecordTransform class is a abstract base class that can be extended to
 * transform records flowing though the system.  The instantiating class is
 * expected to perform application specific transforms on the records.  These
 * might include things like correcting data errors such as fields.  This might
 * also involve much more complicated transforms such as language translations of
 * words, XSLT transformations, etc.
 *
 * @author gfa
 */
public abstract class RecordTransform extends RecordProcessor {

    /**
     * @param input An object representing the record to be transformed
     * @return The transformed record object.
     */
    public abstract RecordCapsule doTransform(RecordCapsule input);

    public RecordCapsule processRecord(RecordCapsule record) {
        return doTransform(record);
    }
}
