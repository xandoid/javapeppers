/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * This is effectively a no-op.  Although it does provide an implementation
 * for the doInitialization method for extenders which do not need to do
 * any initialization.
 *
 * @author gfa
 */
public class NullBuilder extends RecordBuilder {

    /**
     * Dumps out the contents of the RecordCapsule data in a concatenated form.
     *
     * @param r The RecordCapsule object containing the data;
     * @return The incoming RecordCapsule
     */
    public Object buildRecord(RecordCapsule r) {
        return r;
    }

    public boolean doInitialization() {
        return true;
    }

    public Object buildHeader(RecordCapsule r) {

        return new String("");
    }
}
