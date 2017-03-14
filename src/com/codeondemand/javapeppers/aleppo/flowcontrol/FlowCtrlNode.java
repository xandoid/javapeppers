/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;

import java.util.Observable;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.action.AleppoProcess;

/**
 * This class extends AleppoProcess and expects that the AleppoRunnable
 * class that it is monitoring is sending an update via a FlowResult
 * object.
 * 
 * @author gfa
 *
 */
public class FlowCtrlNode extends AleppoProcess {
	
	public void update( Observable o, Object data ){
		logger.debug(AleppoMessages.getString("FlowCtrlNode.0")+ o.getClass().getCanonicalName()); //$NON-NLS-1$
		if( data instanceof FlowResult ){
			super.update(o,((FlowResult)data).getProcess_result());
		}
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowCtrlNode");
	
}
