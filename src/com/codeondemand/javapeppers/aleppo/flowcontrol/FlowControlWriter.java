/**
 *
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.writer.DestinationWriter;
import org.apache.logging.log4j.LogManager;

import java.io.PrintStream;
import java.util.Properties;

/**
 * The FlowControlWriter
 *
 * @author gfa
 */
public class FlowControlWriter extends DestinationWriter {

    public boolean close() {
        return true;
    }

    public boolean reset() {
        return true;
    }

    public boolean write(Object data) {
        boolean retval = false;
        PrintStream outputStream = getPrintStream();
        if (data instanceof RecordCapsule) {
            RecordCapsule rec = (RecordCapsule) data;

            // Check to see if we should write logs and/or stdout
            if (rec.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY) != null) {
                Properties temp = (Properties) rec.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY);
                if (temp.containsKey("flowcontrol.log_results")) {
                    log_results = Boolean.parseBoolean(temp.getProperty("flowcontrol.log_results"));
                }
                if (temp.containsKey("flowcontrol.stdout")) {
                    print_to_stdout = Boolean.parseBoolean(temp.getProperty("flowcontrol.to_stdout"));
                }
            }

            for (int i = 0; i < rec.getFieldCount(); i++) {
                DataCapsule dc = rec.getField(i);
                if (!dc.isNull()) {
                    if (dc.getData() instanceof FlowResult) {
                        FlowResult res = (FlowResult) dc.getData();
                        if (print_to_stdout && outputStream != null) {
                            outputStream.println(dc.getName() + "\n\tresult: " + res.getProcess_result() + "\n\ttime: " + res.getProcess_time() + " milliseconds.");
                            if (res.getResult_data() != null) {
                                outputStream.println("\tmessage: " + res.getResult_data());
                            }
                            if (!res.getProcess_result()) {
                                outputStream.println("error code: " + res.getError_code() + "\n\t");
                                outputStream.println("error reason: " + res.getError_reason());
                            }
                        }
                        if (log_results) {
                            logger.debug(dc.getName() + "\n\tresult: " + res.getProcess_result() + "\n\ttime: " + res.getProcess_time() + " milliseconds.");
                            if (res.getResult_data() != null) {
                                logger.debug("\tmessage: " + res.getResult_data());
                            }
                            if (!res.getProcess_result()) {
                                logger.debug("error code: " + res.getError_code() + "\n\t");
                                logger.debug("error reason: " + res.getError_reason());
                            }
                        }
                    } else {
                        if (!dc.isNull()) {
                            logger.debug(dc.getName() + ":" + dc.getData().toString());
                        }
                    }
                }
            }
            retval = true;
        }
        return retval;
    }

    public void setPrintStream(PrintStream ps) {
        output_stream = ps;
    }

    public PrintStream getPrintStream() {
        return output_stream;
    }

    private PrintStream output_stream = System.out;
    private boolean log_results = false;
    private boolean print_to_stdout = true;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowControlWriter");

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

}
