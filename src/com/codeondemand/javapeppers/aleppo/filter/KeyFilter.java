/**
 *
 */
package com.codeondemand.javapeppers.aleppo.filter;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import java.io.*;
import java.util.TreeMap;

/**
 * The KeyFilter class provides the ability to filter a processing stream by
 * comparing against a set of known keys that are provided either in the form of
 * a file with one line per key, or in the form of a TreeMap. Each Object
 * passing through the filter will have its key checked to see if it matches one
 * of the keys in the set of known keys. Obviously, this method has some
 * limitations for scaling, but should work well for filtering based on hundreds
 * of thousands of keys.
 *
 * @author gfa
 */
public class KeyFilter extends RecordFilter {

    @Override
    protected RecordCapsule filterRecord(RecordCapsule input) {
        RecordCapsule retval = null;
        String key = input.getKeyString();
        if (key.length() == 0 || keys.containsKey(input.getKeyString())) {
            retval = input;
            logger.debug("Record passed->> " + input.toString());
        } else {
            logger.debug("Record filtered->> " + input.toString());
            logger.debug("Key:>" + key + "<");

        }
        return retval;
    }

    public boolean doInitialization() {
        boolean retval = false;
        if (pmap.containsKey("file") && pmap.get("file") instanceof String) {
            String filename = (String) pmap.get("file");
            try (BufferedReader brd = new BufferedReader(new FileReader(new File(filename)))) {

                retval = true;
                while (brd.ready()) {
                    String temp = brd.readLine();
                    if (temp != null) {
                        String key = temp.trim();
                        keys.put(key, 0);
                    }
                }
                brd.close();

            } catch (FileNotFoundException e) {
                logger.error("File containing keys not found: " + filename);
            } catch (IOException e) {
                logger.error("Error reading key file: " + e.toString());
            }

            // report the number of keys loaded.
            logger.debug("Keys loaded: " + keys.size());
        }
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("KeyFilter");

    protected TreeMap<String, Integer> keys = new TreeMap<>();

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }
}
