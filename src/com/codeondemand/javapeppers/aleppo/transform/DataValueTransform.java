/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.transform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import javax.xml.transform.stream.StreamSource;


import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * The DataValueTransform class is used to do name/value translation for the
 * payload coming through in the RecordCapsule. It has a coarse granularity in
 * that it only translates the entire value of a DataCapsule, not for instance
 * words in a String that is the value contained in the DataCapsule.
 * 
 * <code><br>
 * <br>key|name|value
 * <br>...
 * <br>key|name|value
 * </code>
 * <p>
 * The first token is the key to a particular translation map and can be
 * expected to occur multiple times and during translation it is used to locate
 * a DataCapsule in the record that has that name. If it finds a DataCapsule
 * with that name, then it looks at the value in the DataCapsule, and uses that
 * to find a 'name' in the translation table and substitutes a 'value'
 * 
 * For example if our translation file had a line like <code><p>
 * city|Praha|Prague
 * </code>
 * <p>
 * and our incoming record had a DataCapsule with the name 'city' then if it had
 * a value of Praha, then that value would be replaced by the value 'Prague'.
 * 
 * @author Gary Anderson
 * 
 */
public class DataValueTransform extends RecordTransform {

	@Override
	public RecordCapsule doTransform(RecordCapsule input) {
		if (initialized) {
			Iterator<String> it = tables.keySet().iterator();
			while (it.hasNext()) {
				String tag = it.next();
				DataCapsule dc = null;
				if ((dc = input.getField(tag)) != null) {
					logger.debug("Found a map for tag:" + tag);
					if (!dc.isNull()) {
						if (tables.get(tag)
								.containsKey(dc.getData().toString())) {
							dc.setData(tables.get(tag).get(
									dc.getData().toString()));
						}
					} else {
						if (tables.get(tag).containsKey("null")) {
							dc.setData(tables.get(tag).get("null"));
						}
					}
				}
			}
		}
		return input;
	}

	public boolean doInitialization() {
		boolean retval = false;
		if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE) && 
		   pmap.containsKey(AleppoConstants.ALEPPO_DELIMITER_KEY)) {
			initialized = loadTranslationTables((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE),
					(String) pmap.get(AleppoConstants.ALEPPO_DELIMITER_KEY));
			retval = true;
		} else {
			logger.error("Unable to load a translation map, no parameters specified");
		}
		return retval;
	}

	private boolean loadTranslationTables(String file, String delim) {
        boolean retval = false;
		StreamSource source = new StreamSource(ClassLoader
				.getSystemResourceAsStream(file));
		
		BufferedReader rdr = new BufferedReader(new InputStreamReader(source.getInputStream()));
	
		if (rdr != null) {
			retval = true;
			logger.debug("Parsing translation table with delimiter: " + delim);
			String line = null;
			try {
				while ((line = rdr.readLine()) != null) {
					StringTokenizer t = new StringTokenizer(line.trim(), delim);
					logger.debug("Token count=" + t.countTokens());
					if (t.countTokens() == 3) {
						String key = t.nextToken();
						String name = t.nextToken();
						if (name.length() == 0) {
							name = "null";
						}
						String value = t.nextToken();
						if (tables.containsKey(key)) {
							TreeMap<String, String> temp = tables.get(key);
							temp.put(name, value);
							logger.debug("Adding k/v pair for key: " + key + ":"
									+ name + ":" + value);
						} else {
							TreeMap<String, String> temp = new TreeMap<String, String>();
							temp.put(name, value);
							logger.debug("Adding map for key:" + key);
							tables.put(key, temp);
							logger.debug("Adding k/v pair for key: " + key + ":"
									+ name + ":" + value);
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return retval;
	}

	private boolean initialized = false;
	private TreeMap<String, TreeMap<String, String>> tables = new TreeMap<String, TreeMap<String, String>>();

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DataValueTransform");


	public void done() {
		// TODO Auto-generated method stub
		
	}
}
