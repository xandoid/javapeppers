package com.codeondemand.javapeppers.aleppo.builder;

import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

/**
 * Converts a RecordCapsule object into a String that is expressed as JSON.
 * 
 * @author gfa
 *
 */
public class JSONBuilder extends NullBuilder {

	public Object buildRecord(RecordCapsule r) {
	
		RecordCapsule retval = r;
		//System.out.println( r.toString());
		String bar = buildJSON(r);
		if( bar != null ){
			r.addDataCapsule(new DataCapsule(outputField, bar), false);
		}
		return retval;
	}

	public Object buildHeader(RecordCapsule r) {
		// TODO Auto-generated method stub
		return "";
	}

	private String buildJSON(RecordCapsule rc) {
		String retval = null;
		JsonBuilderFactory factory = Json.createBuilderFactory(null);
		JsonObjectBuilder bldr = factory.createObjectBuilder();

		for (int i = 0; i < fieldList.size(); i++) {
			String foo = fieldList.get(i);
			if (rc.checkField(foo)) {
				bldr.add(foo, rc.getField(foo).getData().toString());
			}
		}
		if (doTS) {
			bldr.add("timestamp", MiscUtil.getCurrentTimeString());
		}
		retval = bldr.build().toString();
		return retval;
	}

	@Override
	public boolean doInitialization() {
		// TODO Auto-generated method stub
		if (pmap.containsKey("fields")) {
	
			StringTokenizer stok = new StringTokenizer( (String) pmap.get("fields"),"|");
			while( stok.hasMoreTokens()){
				fieldList.add(stok.nextToken());
			}
		
		}
		if( pmap.containsKey("doTS")){
			doTS = Boolean.parseBoolean((String)pmap.get("doTS"));
		}
		
		if (pmap.containsKey("outputField")) {
			outputField = (String) pmap.get("outputField");
		}

		return true;
	}
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("JSONBuilder");
	private ArrayList<String> fieldList = new ArrayList<String>();
	private String outputField = null;
	private boolean doTS = false;

}
