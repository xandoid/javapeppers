/**
 *
 */
package com.codeondemand.javapeppers.aleppo.parser;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

import java.util.Properties;
import java.util.TreeMap;

public interface RecordParser {

    RecordCapsule parseRecord(Object input);

    void setProperties(Properties props);

    boolean initialize(TreeMap<String, Object> attributes);
}
