/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.action;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
/**
 * This class provides a place holder that can be used as a null
 * processing step in a dataflow.  It always sends a notification
 * of success -- notifyObserver(true) -- before it exits the 
 * run method.
 * 
 * It logs the properties it is passed to the debug stream of 
 * the logger.
 * 
 * @author gfa
 *
 */
public class NullRunnable extends AleppoRunnable {

	@Override
	public void run() {
		Properties p = null;
		if( processData != null && 
		    (p = (Properties) processData.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY)) != null )
		{
			java.util.Iterator<Object> it = p.keySet().iterator();
			while( it.hasNext()){
				String key = (String) it.next();
				logger.debug(key +":"+ p.getProperty(key));			
			}
		}
		setChanged();
		notifyObservers(true);		
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("NullRunnable");
	
}
