/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import org.apache.logging.log4j.LogManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Implementation of SourceReader in the context of reading records from stdin.
 *
 * @author gfa
 */
public class StdSourceReader extends SourceReader {

    public boolean close() {
        return true;
    }

    public boolean reset() {
        return true;
    }

    public Object read() {
        String retval = null;
        try {
            if (rdr.ready()) {
                retval = rdr.readLine();
            } else {
                logger.debug("Waiting for stdin");
                Thread.yield();
                if (wait) {
                    while (!rdr.ready()) {
                        Thread.sleep(10);
                        Thread.yield();
                    }
                }
                Thread.sleep(10);
                if (rdr.ready()) {
                    retval = rdr.readLine();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return retval;
    }

    @Override
    public boolean doInitialization() {
        return true;
    }

    private boolean wait = false;
    private BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("StdSourceReader");


}
