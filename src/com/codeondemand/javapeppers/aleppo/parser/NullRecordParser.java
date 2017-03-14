/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.parser;


import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FlowNode;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public class NullRecordParser extends FlowNode implements RecordParser {

	public RecordCapsule parseRecord(Object input) {
		RecordCapsule retval = null;
		logger.debug("Parsing: "+input);
		if (input != null) {
			if (input instanceof RecordCapsule) {
				retval = (RecordCapsule) input;
			} else {
				String recname = "record";
				String fieldname = "data";
				if( pmap != null && pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_RECORD)){
					recname = (String)pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_RECORD);
				}
				if( pmap != null && pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD)){
					fieldname = (String)pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FIELD);
				}
				DataCapsule foo = new DataCapsule(fieldname, input);
				retval = new RecordCapsule(recname,null);
				retval.addDataCapsule(foo, false);
			}
		}
		return retval;
	}

	@Override
	public boolean doInitialization() {
		return true;
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("NullRecordParser");

}
