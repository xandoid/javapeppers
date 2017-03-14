/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.parser;

import java.util.Properties;
import java.util.TreeMap;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public interface RecordParser {
	
	public RecordCapsule parseRecord(Object input );
		
	public void setProperties(Properties props);

	public boolean initialize(TreeMap<String, Object> attributes);
}
