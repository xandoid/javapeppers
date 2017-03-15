/**
 *
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;


import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import org.apache.logging.log4j.LogManager;

/**
 * This class allows place holder steps to be put into process flows.
 * The step it represents will always be successful unless the value of
 * the 'step_result' parameter in the configuration file for the node has a
 * value of 'false'.
 *
 * @author gfa
 */
public class NullFlowProcessStep extends FlowProcessStep {

    @Override
    protected void doRun() {
        if (pmap.containsKey("stepname")) { //$NON-NLS-1$
            logger.debug(AleppoMessages.getString("NullFlowProcessStep.1") + pmap.get("stepname")); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            logger.debug(AleppoMessages.getString("NullFlowProcessStep.3"));             //$NON-NLS-1$
        }
    }

    /**
     * This value is controlled by the value of the 'result' parameter in
     * the configuration block for the node that this step represents.  If
     * there is no parameter value for 'result', then this method will
     * return the default value of true.
     *
     * @return Returns true unless a parameter 'result' is specified in the
     * configuration file for this step, then it will return the
     * boolean value of that parameter.
     */
    protected boolean isSuccessfulRun() {
        boolean retval = true;
        if (pmap.containsKey("step_result")) { //$NON-NLS-1$
            retval = Boolean.parseBoolean((String) pmap.get("step_result")); //$NON-NLS-1$
        }
        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("NullFlowProcessStep");

}
