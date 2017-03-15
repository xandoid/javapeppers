/**
 *
 */
package com.codeondemand.javapeppers.aleppo.parser;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

import java.util.Properties;
import java.util.TreeMap;

public interface RecordParser {

    public RecordCapsule parseRecord(Object input);

    public void setProperties(Properties props);

    public boolean initialize(TreeMap<String, Object> attributes);
}
