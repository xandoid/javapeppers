/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.reader;

import java.util.ArrayList;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.sambal.fields.DataField;

public class SambalReader extends SourceReader {

	public boolean initialize(ArrayList<DataField> f) {
		boolean retval = false;
		if (f != null) {
			fieldSpecs = f;
			retval = true;
		}
		return retval;
	}

	public boolean close() {
		return true;
	}

	public RecordCapsule read() {
		RecordCapsule retval = new RecordCapsule("record", "record");
		for (int i = 0; i < fieldSpecs.size(); i++) {
			if (fieldSpecs.get(i) instanceof DataField) {
				DataField f = (DataField) fieldSpecs.get(i);
				if (firstread) {
					retval.addDataCapsule(new DataCapsule(f.getName(), f
							.getName()), false);

					firstread = false;
				} else {
					retval.addDataCapsule(new DataCapsule(f.getName(), f
							.getNextValue()), false);
				}
			}
		}
		return retval;
	}

	public boolean reset() {
		firstread = true;
		return true;
	}
	
	@Override
	public boolean doInitialization() {
		return false;
	}


	private ArrayList<DataField> fieldSpecs = null;
	private boolean firstread = true;
	//private RecordCapsule currentRecord = null;

}
