package com.codeondemand.javapeppers.aleppo.flowcontrol;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.connector.RecordConnector;
import com.codeondemand.javapeppers.aleppo.process.ConfigurationLoader;
import com.codeondemand.javapeppers.aleppo.process.ProcessLauncher;

/**
 * The concept of a SubFlow allows us to launch a completely independent dataflow at any step in
 * the existing dataflow.  It is similar to a subprocess launched from a script, but of course it
 * lives within the existing JVM context.
 * 
 * This 
 * 
 * @author gfa
 *
 */
public class SubFlow extends FlowProcessStep{

	@Override
	protected void doRun() {
		if( pmap!= null && pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE)) {
			ConfigurationLoader cfl = new ConfigurationLoader();
			ArrayList<RecordConnector> temp =
				    cfl.initialize((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE));
			if( temp == null){
				logger.error(AleppoMessages.getString("SubFlow.0")); //$NON-NLS-1$
				isSuccessfulRun = false;
			}else{
				logger.debug(AleppoMessages.getString("SubFlow.1")+pmap.get("stepname")); //$NON-NLS-1$ //$NON-NLS-2$
				try{
					ProcessLauncher p = new ProcessLauncher();   
					result.setResult_data(AleppoMessages.getString("SubFlow.3")); //$NON-NLS-1$
					isSuccessfulRun = p.process(temp);						
				}catch( Exception e){
					logger.error(e.toString());
					result.setResult_data(AleppoMessages.getString("SubFlow.5")); //$NON-NLS-1$
					isSuccessfulRun = false;
				}
			}
		}else{
			logger.error(AleppoMessages.getString("SubFlow.4")); //$NON-NLS-1$
		}
//		notify( result);
//		setChanged();
//		notifyObservers(isSuccessfulRun);
	}

	@Override
	protected boolean isSuccessfulRun() {
		return isSuccessfulRun;
	}
	boolean isSuccessfulRun = false;
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SubFlow");
}
