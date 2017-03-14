/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.builder;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public class SpecificationBuilder extends NullBuilder {

	public SpecificationBuilder(String delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public Object buildRecord(RecordCapsule rc ){
		String retval = null;
		if( rc != null){
			logger.debug(rc.toString());
			int start = 1;
			int pos = 1;
			String temp = ""; //$NON-NLS-1$
			StringBuffer sb = new StringBuffer();
			for( int i = 0 ; i < rc.getFieldCount(); i++ ){
				DataCapsule dc = rc.getField(i);
				
				if( dc != null ){
					sb.append(temp+dc.getName());
					temp=delimiter;
					if( dc.getMetaData("typeName") != null){
						sb.append(temp+dc.getMetaData("typeName")); //$NON-NLS-1$						
					}else{
						sb.append(temp+"CHARACTER");
					}
					sb.append(temp+pos); //$NON-NLS-1$
					sb.append(temp+(start));
					int len = 1;
					if( dc.getMetaData("length") != null ){
						len = Integer.parseInt((dc.getMetaData("length").toString())); //$NON-NLS-1$
					}
					start += len;
					sb.append(temp+len);
					sb.append(temp+pos); //$NON-NLS-1$
					if( dc.getMetaData("isKey") != null ){
						sb.append(temp+Boolean.parseBoolean(dc.getMetaData("isKey").toString())); //$NON-NLS-1$					
					}else{
						sb.append(temp+"false"); //$NON-NLS-1$											
					}
					pos++;
				}
				sb.append("\n"); //$NON-NLS-1$
				temp = ""; //$NON-NLS-1$
			}
			if( sb.length() > 0){
				retval = sb.toString();				
			}
		}
		return retval;
	}
	protected String delimiter = "";
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SpecificationBuilder");
	
}
