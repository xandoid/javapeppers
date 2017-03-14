package com.codeondemand.javapeppers.aleppo.source;

import java.util.Vector;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
/**
 * The AggregateSource class provides the functionality to concatenate a number 
 * sources into a single source.  This implementation of the class does not 
 * consider the record keys, so it just is a simple concatenator for as many 
 * sources as it finds.  When one source is exhausted, then the class will 
 * return a null for the next record request.
 * 
 * @author gfa
 *
 */
public class AggregateSource extends RecordSource {

	/**
	 * Closes all of the underlying record sources.
	 */
	@Override
	public boolean closeSource() {
		boolean retval = false;
		if (sources != null) {
			for (int i = 0; i < sources.size(); i++) {
				((RecordSource) sources.get(i)).closeSource();
			}
			retval = true;
		}
		return retval;
	}

	@Override
	public RecordCapsule getCurrentRecord() {
		RecordCapsule retval = null;
		if (sources != null) {
			for (int i = 0; i < sources.size(); i++) {
				retval = combine(retval, ((RecordSource) sources.get(i))
						.getCurrentRecord());
			}
		}
		return retval;
	}

	@Override
	public RecordCapsule getHeaderRecord() {
		RecordCapsule retval = null;
		if (sources != null) {
			for (int i = 0; i < sources.size(); i++) {
				retval = combine(retval, ((RecordSource) sources.get(i))
						.getHeaderRecord());
			}
		}
		return retval;
	}

	@Override
	public RecordCapsule getNextRecord() {
		RecordCapsule retval = null;
		boolean finished = false;
		if (sources != null) {

			for (int i = 0; i < sources.size(); i++) {
				if (((RecordSource) sources.get(i)).getNextRecord() == null) {
					finished = true;
					retval = null;
					break;
				}
			}
			if (!finished) {
				for (int i = 0; i < sources.size(); i++) {
					logger.debug(((RecordSource)sources.get(i)).getCurrentRecord());
					retval = combine(retval, ((RecordSource) sources.get(i))
							.getCurrentRecord());
				}
			}
		}
		return retval;
	}


	public boolean initialize(Object[] args) {
		boolean retval = false;
		if (args != null && args.length > 0 && args[0] instanceof RecordSource) {
			sources = new Vector<RecordSource>(args.length);
			for (int i = 0; i < args.length; i++) {
				if (args[i] instanceof RecordSource) {
					sources.add((RecordSource) args[i]);
					logger.debug("Adding record source" +args[i].toString());
				} else {
					sources.clear();
					retval = false;
					break;
				}
				retval = true;
			}
		}

		return retval;
	}

	@Override
	public boolean reset() {
		boolean retval = false;
		if (sources != null) {
			for (int i = 0; i < sources.size(); i++) {
				((RecordSource) sources.get(i)).reset();
			}
			retval = true;
		}
		return retval;
	}

	protected RecordCapsule combine(RecordCapsule current, RecordCapsule append) {
		RecordCapsule retval = current;

		if (retval == null) {
			retval = append;
		} else {
			for( int i = 0 ; i < append.getFieldCount();i++ ){
					current.addDataCapsule(append.getField(i), false);
			}
		}
		return retval;
	}

	public boolean setSource(RecordSource src ){
		boolean retval = false;
		if( sources == null ){
			sources = new Vector<RecordSource>();
			retval = sources.add(src);
		}else{
			sources.add(src);
		}
		
		return retval;
	}
	protected Vector<RecordSource> sources = null;
	protected Object delim = new String();
	
	
	public Object[] getCurrentRecordNoKey() {
		return null;
	}

	public Object getHeaderRecordNoKeys() {
		return null;
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("AggregateSource");
}
