/**
 *
 */
package com.codeondemand.javapeppers.aleppo.writer;

/**
 * The NullWriter is a one-way trip to the bit bucket, but always indicates
 * success at the task.
 *
 * @author gfa
 */
public class NullWriter extends DestinationWriter {

    public boolean close() {
        return true;
    }

    public boolean reset() {
        return close();
    }

    public boolean write(Object record) {
        return true;
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }
}
