/**
 *
 */
package com.codeondemand.javapeppers.aleppo.writer;

import org.apache.logging.log4j.LogManager;

/**
 * The LogRecordWriter outputs the string representation of an incoming record
 * using the current settings specified in the log4j.properties file that is
 * active for this process. The output will only be written if the INFO level
 * is active for this class.
 *
 * @author gfa
 */
public class LogRecordWriter extends DestinationWriter {

    public boolean close() {
        return true;
    }

    public boolean reset() {
        return true;
    }

    public boolean write(Object record) {
        if (logger.isInfoEnabled()) {
            logger.info(record.toString());
        }
        return true;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("LogRecordWriter");

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }
}
