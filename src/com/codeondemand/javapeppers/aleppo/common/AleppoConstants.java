/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.common;

public class AleppoConstants {

	// Specify some constant strings for the meta-data used in DataCapsules
	public final static String ALEPPO_DC_MDATA_TYPE_KEY       = "typeName";
	public final static String ALEPPO_DC_MDATA_LENGTH_KEY     = "length";
	public final static String ALEPPO_DC_MDATA_PRECISION_KEY      = "precision";
	public final static String ALEPPO_DC_MDATA_PROPERTIES_KEY = "properties";

	// Specify some default constants for looking up and fetching things.
	public final static String ALEPPO_DELIMITER_KEY = "delimiter";
	public final static String ALEPPO_DB_SCHEMA_KEY = "schema";
	public final static String ALEPPO_DB_TABLE_KEY = "table";
	public final static String ALEPPO_CLASS_RUNNABLE_KEY = "runnable";

	public final static String ALEPPO_FILTER_PCT_KEY = "pct";

	// Strings related to transform processors.
	public final static String ALEPPO_TRANSFORM_CONVERSION_KEY = "conversion";
	public final static String ALEPPO_TRANSFORM_XFORMNAMES_KEY = "xformnames";
	
	// Specify some default values
	public final static String ALEPPO_DELIMITER_DEFAULT = "|";
	public final static String ALEPPO_DB_SCHEMA_DEFAULT = "_SCHEMA_";
	public final static String ALEPPO_DB_TABLE_DEFAULT = "_TABLE_";

	// Specify the tags in an aleppo configuration file
	public final static String ALEPPO_CONFIG_DATAFLOW_TAG = "dataflow";
	public final static String ALEPPO_CONFIG_CONNECTOR_TAG = "connector";
	public final static String ALEPPO_CONFIG_AGGREGATOR_TAG = "aggregator";
	public final static String ALEPPO_CONFIG_CONCENTRATOR_TAG = "concentrator";
	public final static String ALEPPO_CONFIG_OBSERVERS_TAG = "observers";
		public final static String ALEPPO_CONFIG_SOURCE_TAG = "source";
	public final static String ALEPPO_CONFIG_DESTINATION_TAG = "destination";
	public final static String ALEPPO_CONFIG_PROCESS_TAG = "process";
	public final static String ALEPPO_CONFIG_ACTION_TAG = "action";
	public final static String ALEPPO_CONFIG_RECORD_TAG = "record";
	public final static String ALEPPO_CONFIG_FIELD_TAG = "field";
	public final static String ALEPPO_CONFIG_FILTER_TAG = "filter";
	public final static String ALEPPO_CONFIG_MONITOR_TAG = "monitor";
	public final static String ALEPPO_CONFIG_TRANSFORM_TAG = "transform";
	public final static String ALEPPO_CONFIG_READER_TAG = "reader";
	public final static String ALEPPO_CONFIG_PARSER_TAG = "parser";
	public final static String ALEPPO_CONFIG_BUILDER_TAG = "builder";
	public final static String ALEPPO_CONFIG_WRITER_TAG = "writer";
	public final static String ALEPPO_CONFIG_XX_TAG = "xx";
	

	
	// Specify specific parameter names for configuration
	public final static String ALEPPO_CONFIG_CONNECTOR_PARAM_DOHEADER = "doheader";
	public final static String ALEPPO_CONFIG_ALL_PARAM_MAXROWS = "maxrows";
	public final static String ALEPPO_CONFIG_ALL_PARAM_MINROWS = "minrows";
	public final static String ALEPPO_CONFIG_ALL_PARAM_CLASS = "class";
	public final static String ALEPPO_CONFIG_ALL_PARAM_FILE = "file";
	public final static String ALEPPO_CONFIG_ALL_PARAM_NAME = "name";
	public final static String ALEPPO_CONFIG_ALL_PARAM_ISKEY = "iskey";
	public final static String ALEPPO_CONFIG_ALL_PARAM_RECORD = "record";
	public final static String ALEPPO_CONFIG_ALL_PARAM_FIELD = "field";
	public static final String ALEPPO_CONFIG_ALL_PARAM_TYPE = "type";
	public static final String ALEPPO_CONFIG_ALL_PARAM_TOKENIZE = "tokenize";
	
}
