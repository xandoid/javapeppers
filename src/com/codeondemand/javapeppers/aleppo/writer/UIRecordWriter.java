/**
 *
 */
package com.codeondemand.javapeppers.aleppo.writer;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public abstract class UIRecordWriter extends DestinationWriter {

    public abstract boolean initialize(RecordCapsule rc);
}
