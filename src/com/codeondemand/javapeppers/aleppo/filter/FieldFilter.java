package com.codeondemand.javapeppers.aleppo.filter;

import java.util.ArrayList;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

/**
 * This filter allows either passing or blocking of a record
 * based on whether the value of one of the DataCapsules contains
 * any one of a specified set of strings.
 * 
 * The name of the DataCapsule to be checked is specified in the
 * pmap string 'field' and the filter values are specified in the
 * pmap string 'values' (pipe delimited).  If the DataCapsule 
 * contains a valid value, then the record is passed on to the 
 * next processing step.
 * 
 * @author gfa
 *
 */
public class FieldFilter extends RecordFilter {

	@Override
	protected RecordCapsule filterRecord(RecordCapsule input) {
		RecordCapsule retval = null;
		
		if( pmap.containsKey("field") && pmap.containsKey("values")){
			String field = (String) pmap.get("field");
			String values = (String)pmap.get("values");
			values = MiscUtil.mapString(props, values, "%");
			String delim = "|";
			if( pmap.containsKey("delim")){
				delim = (String)pmap.get("delim");
			}
			ArrayList<String>foo = MiscUtil.StringToList(values, delim);
			
			if( input.getField(field) != null && !input.getField(field).isNull()){
				String temp = input.getField(field).getData().toString();
				if( foo.contains(temp)){
					retval = input;
				}
			}	
		}
		return retval;
	}

	@Override
	public boolean doInitialization() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}

}
