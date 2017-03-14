/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * The NullSourceReader provides simple functionality if you have a generic data 
 * that does not depend on a source.  If you provide parameters in the configuration
 * file for 'fieldvalue' and 'fieldname', each RecordCapsule generated will contain
 * a DataCapsule with the value of 'fieldname' containing data specified by 
 * 'fieldvalue'.  This information can then be used by downstream RecordProcessors.
 * 
 * Each RecordCapsule emitted by this reader will be named record_x where 'x' starts
 * at zero and increments by one for each record emitted.
 * 
 * @author gfa
 *
 */
public class NullSourceReader extends SourceReader{

	@Override
	public boolean close() {
		return true;
	}

	@Override
	public Object read() {
		RecordCapsule rc = new RecordCapsule("record_"+counter++,null);
		if( pmap.containsKey("fieldname") ){
			DataCapsule dc = new DataCapsule((String) pmap.get("fieldname"),null);
			if( pmap.containsKey("fieldvalue")){
				dc.setData(pmap.get("fieldvalue"));
			}
			rc.addDataCapsule(dc, false);
		}
		return rc;
	}

	@Override
	public boolean reset() {
		return true;
	}

	@Override
	public boolean doInitialization() {
		return true;
	}

	protected int counter = 0;
}
