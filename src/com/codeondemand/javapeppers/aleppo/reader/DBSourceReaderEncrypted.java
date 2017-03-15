/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;


import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

/**
 * This class is an implementation of SourceReader in the context of a database
 * read.
 *
 * @author gfa
 */
public class DBSourceReaderEncrypted extends DBSourceReader {


    protected String decryptPWD(String input) {
        String retval = null;
        retval = MiscUtil.decodeB64String(input);
        logger.debug("input:" + input + " output: " + retval);
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DBSourceReaderEncrypted");


}
