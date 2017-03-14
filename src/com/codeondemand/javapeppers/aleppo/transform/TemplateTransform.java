/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public class TemplateTransform extends RecordTransform {

	@Override
	public RecordCapsule doTransform(RecordCapsule input) {
		RecordCapsule retval = input;
		if( pmap.containsKey("append") &&  pmap.get("append").toString().equals("yes")){
				retval = input;
		}else{
			retval = new RecordCapsule("record",null );
		}	
		
		String temp = new String(template);
		for( int i = 0 ; i < input.getFieldCount(); i++){
			DataCapsule dc = input.getField(i);
			if( !dc.isNull()){

				String name = "@"+dc.getName().trim()+"@";
				if( temp.contains(name) ){
					temp = temp.replace(name, dc.getData().toString());
				}
			}
		}
		retval.addDataCapsule(new DataCapsule(output_tag,temp),false);
		
		return retval;
	}

	@Override
	public boolean doInitialization() {
		   boolean retval = false;
			if( pmap.containsKey("file") && pmap.containsKey("output_tag")){
				template = MiscUtil.fileToString((String)pmap.get("file"));
				output_tag = pmap.get("output_tag").toString();
				if( template != null && output_tag != null) {
					retval = true;					
				}
			}
		   return retval;
	}
	
	

	protected String template = null;
	protected String output_tag = null;
	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}
}
