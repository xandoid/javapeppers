/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.FlowNode;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public abstract class RecordBuilder extends FlowNode {

    /**
     * Generic interface to format the data encapsulated by the RecordCapsule into an
     * object suitable for output by a DestinationWriter object.
     *
     * @param r The incoming RecordCapsule
     * @return An object suitable for output to a destination.
     */
    public abstract Object buildRecord(RecordCapsule r);

    /**
     * Generic interface to format the meta-data encapsulated by the RecordCapsule into an
     * object suitable for output by a DestinationWriter object.
     *
     * @param r The incoming RecordCapsule
     * @return An object suitable for storing to a destination.
     */
    public abstract Object buildHeader(RecordCapsule r);

}
