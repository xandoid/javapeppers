/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public abstract class UISourceReader extends SourceReader {

    public abstract boolean initialize(RecordCapsule rc);

    public boolean close() {
        return false;
    }

    public Object read() {
        return null;
    }

    public boolean reset() {
        return false;
    }

}
