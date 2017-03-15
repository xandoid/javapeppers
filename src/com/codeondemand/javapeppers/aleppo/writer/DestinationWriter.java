/**
 *
 */
package com.codeondemand.javapeppers.aleppo.writer;

import java.util.Properties;
import java.util.TreeMap;

/**
 * This interface provides just simple functionality for outputting the value of
 * some Object to some destination target, which might be a queue, file,
 * database, bit bucket, etc.
 * <p>
 * The implementing class should maintain the state of the target. There is not
 * requirement that the target remain open between successive calls to the write
 * method, but if not, it will need to reopen the target each time a write is
 * requested.
 *
 * @author gfa
 */
public abstract class DestinationWriter {

    /**
     * This method should implement whatever tasks need to be done to terminate
     * the destination, such as flushing and closing a file, closing a database
     * connection, etc.
     *
     * @return true if successful, otherwise false.
     */
    public abstract boolean close();

    /**
     * This method should return the destination to its state when it was first
     * opened. For instance, a file might be closed and then reopened, etc.
     *
     * @return true if successful
     */
    public abstract boolean reset();

    /**
     * This method should implement a write to the destination and output the
     * complete contents of the incoming data.
     *
     * @param data An object that is meaningful to the implementing class and
     *             contains the data to be written.
     * @return true if successful otherwise false.
     */
    public abstract boolean write(Object data);

    public abstract void activate();


    public void setProperties(Properties props) {
        this.props = props;
    }

    public Properties getProperties() {
        return this.props;
    }

    public TreeMap<String, Object> getPmap() {
        return pmap;
    }

    public void setPmap(TreeMap<String, Object> pmap) {
        this.pmap = pmap;
    }

    protected Properties props = null;
    protected TreeMap<String, Object> pmap = null;
}
