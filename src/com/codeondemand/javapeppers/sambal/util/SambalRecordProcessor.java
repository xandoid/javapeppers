/**
 * 
 */
package com.codeondemand.javapeppers.sambal.util;

import java.util.ArrayList;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.sambal.fields.TokenField;

public abstract class SambalRecordProcessor extends RecordProcessor {

	public boolean doInitialization(){
		return initializeFields();
	}
	
	public RecordCapsule processRecord(RecordCapsule recin){
		if( !initialized ){
			initialized = initializeFields();
		}
		
		for( int i = 0; i < fields.size(); i++){
			recin.addDataCapsule(new DataCapsule(fields.get(i).getName(),fields.get(i).getNextValue()), false);
		}
		return recin;
	}
	
	public abstract boolean initializeFields();
	
	private boolean initialized = false;
	protected ArrayList<TokenField> fields = null; 
}
