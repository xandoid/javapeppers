package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

public class SplitLabeller extends RecordProcessor {

	@Override
	public RecordCapsule processRecord(RecordCapsule input) {
		input.addDataCapsule(new DataCapsule(split_field,new Integer(split_value)), false);
		if( split_value < splits){
			split_value = split_value +1;
		}else{
			split_value=1;
		}
		return input;
	}

	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean doInitialization() {
		
		// Allow customization of the field that is used for storing the split.
		if( pmap.containsKey("split_label")){
			split_field=(String)pmap.get("split_label");
		}
		if( pmap.containsKey("split_count")){
			String temp = (String)pmap.get("split_count");
			if( temp != null && temp.length() > 0){
				splits = Integer.parseInt(temp);
			}
		}
		
		return true;
	}

	// Default the name of the field for holding the split number to 'split'
	private String split_field = "split";
	
	// Default number of splits to 1
	private int splits= 1;
	
	// Start with split value of 0
	private int split_value = 1;
}
