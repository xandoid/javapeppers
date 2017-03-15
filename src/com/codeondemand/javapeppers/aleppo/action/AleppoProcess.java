/**
 *
 */
package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.aleppo.flowcontrol.FlowResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Observable;
import java.util.Observer;

/**
 * An AleppoProcess extends the functionality of a RecordProcessor to allow the
 * processing to be done on separate thread. This allows you to create a
 * processing class by extending the AleppoRunnable class.
 * <p>
 * Typically, the approach of AleppoProcess/AleppoRunnable is used when you are
 * passing just a single token through a process flow and doing some complex
 * processing at each step, although it is also a convenient way to wrap
 * existing classes for use by the Aleppo framework.
 *
 * @author gfa
 */
public class AleppoProcess extends RecordProcessor implements Observer {

    public RecordCapsule processRecord(RecordCapsule input) {
        RecordCapsule retval = null;

        if (!this.initialized) {
            this.initialized = doInitialization();
            if (!this.initialized) {
                logger.error(AleppoMessages.getString("AleppoProcess.0")); //$NON-NLS-1$
            }
        }

        if (process != null) {

            logger.debug(AleppoMessages.getString("AleppoProcess.1") + process.toString()); //$NON-NLS-1$

            process.setParameters(pmap);
            process.setProcessData(input);

            Thread procThread = new Thread(process);

            procThread.start();
            try {
                procThread.join();
            } catch (InterruptedException e) {
                logger.error(e.toString());
            }
        } else {
            logger.error(AleppoMessages.getString("AleppoProcess.2")); //$NON-NLS-1$
        }

        // Only return a non-null value if the underlying Runnable successfully
        // completed with a 'true' for process_result.
        if (process_result) {

            retval = input;
            input.addDataCapsule(new DataCapsule("Process Result for " + runnable_name + " : ", process_result), false);
            if (process != null) {
                logger.debug(AleppoMessages.getString("AleppoProcess.3") + process.getClass().toString() + AleppoMessages.getString("AleppoProcess.4"));
            }
        } else {
            if (process != null) {
                logger.debug(AleppoMessages.getString("AleppoProcess.5") + //$NON-NLS-1$
                        process.getClass().toString() + AleppoMessages.getString("AleppoProcess.6")); //$NON-NLS-1$

            }
        }
        return retval;
    }

    /**
     * The doInitialization method involves the instantiation of the underlying
     * AleppoRunnable class specified in the configuration file using a
     * 'runnable' parameter in the action element.
     * <p>
     * Note: The doInitialization method does not start the process, only
     * instantiates the class and adds itself as an observer.
     */
    public boolean doInitialization() {
        boolean retval = false;
        if (pmap.containsKey(AleppoConstants.ALEPPO_CLASS_RUNNABLE_KEY)) {
            runnable_name = (String) pmap.get(AleppoConstants.ALEPPO_CLASS_RUNNABLE_KEY);
            try {
                // Attempt to create the specified object.
                Object temp = Class.forName(runnable_name).newInstance();
                if (temp instanceof AleppoRunnable) {
                    process = (AleppoRunnable) temp;
                    process.addObserver(this);
                    logger.debug(AleppoMessages.getString("AleppoProcess.7")  //$NON-NLS-1$
                            + process.getClass().toString());
                    retval = true;
                } else {

                    // If not a runnable, then dump the object that was created.
                    temp = null;
                    logger.error(AleppoMessages.getString("AleppoProcess.8")); //$NON-NLS-1$
                }
            } catch (InstantiationException e) {
                logger.error(e);
            } catch (IllegalAccessException e) {
                logger.error(e);
            } catch (ClassNotFoundException e) {
                logger.error(e);
            }
        }
        return retval;
    }

    /**
     * Records the outcome of the AleppoRunnable executed by this AleppoProcess.
     *
     * @param o   The Observable Object which sent the message
     * @param arg The notification information
     */
    public void update(Observable o, Object arg) {

        if (arg instanceof Boolean) {
            process_result = ((Boolean) arg).booleanValue();
        } else if (arg instanceof FlowResult) {
            process_result = ((FlowResult) arg).getProcess_result();
            logger.debug(AleppoMessages.getString("AleppoProcess.9") + o.getClass().toString() + //$NON-NLS-1$
                    AleppoMessages.getString("AleppoProcess.10") + arg.toString()); //$NON-NLS-1$
        }

        String message = AleppoMessages.getString("AleppoProcess.11"); //$NON-NLS-1$
        // I don't think the Observable could ever be null, but just in case.
        if (o != null) {
            message = message + o.getClass().toString() + AleppoMessages.getString("AleppoProcess.12"); //$NON-NLS-1$
        }

        if (arg != null) {
            message = message + arg.toString();
        } else {
            message = message + AleppoMessages.getString("AleppoProcess.13"); //$NON-NLS-1$
        }

        logger.debug(message);
    }

    private boolean initialized = false;
    private AleppoRunnable process = null;
    private boolean process_result = false;
    private String runnable_name = null;

    // Class specific log4j logger
    private static final Logger logger = LogManager.getLogger("AleppoProcess");

    public void done() {
        // TODO Auto-generated method stub

    }

}
