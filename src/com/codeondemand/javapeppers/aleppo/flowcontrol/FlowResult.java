/**
 *
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;

/**
 * The FlowResult class provides a simple encapsulation of the results of single
 * step in a process flow.  It is intended to be maintained by FlowProcessStep
 * objects as a means to capture the results from each step and pass the information
 * along with the RecordCapsule that is acting as the process flow token.
 *
 * @author gfa
 */
public class FlowResult {

    /**
     * Indicates whether the process was successful or not, assuming that there
     * was a call made to the setProcess_result method.
     *
     * @return true if the process step was successful, otherwise false.  The
     * default value will be false if there was never a call to
     * the setProcess_result step.
     */
    public Boolean getProcess_result() {
        return process_result;
    }

    /**
     * Sets the process result flag, which is set to false unless this method
     * is called to set it to true.
     *
     * @param process_result Set to true if the process was successful.
     */
    public void setProcess_result(Boolean process_result) {
        this.process_result = process_result;
    }

    /**
     * Returns any error code that was set by a process step.
     *
     * @return An error code as a String
     */
    public String getError_code() {
        return error_code;
    }

    /**
     * Sets the error code string.
     *
     * @param error_code
     */
    public void setError_code(String error_code) {
        this.error_code = error_code;
    }

    /**
     * Fetches the error reason, if it was set, otherwise it will
     * return a null.
     *
     * @return Returns null if never set, otherwise returns the reason
     * for the error if any as a String.
     */
    public String getError_reason() {
        return error_reason;
    }

    /**
     * Sets a String representation for the error reason.
     *
     * @param error_reason A String representing some meaningful error information.
     */
    public void setError_reason(String error_reason) {
        this.error_reason = error_reason;
    }

    /**
     * Returns the process time in milliseconds, assuming the call to setProcess_time
     * was set using millisecond units.
     *
     * @return Returns the process time for the flow step in milliseconds.
     */
    public Long getProcess_time() {
        return process_time;
    }

    /**
     * Sets the process time for a flow step, this should be set in millisecond units.
     *
     * @param process_time The number of milliseconds that the process consumed.
     */
    public void setProcess_time(Long process_time) {
        this.process_time = process_time;
    }

    /**
     * Returns the result data as a String.
     *
     * @return A String representing the result data.
     */
    public String getResult_data() {
        return result_data;
    }

    /**
     * Allows setting some text as result data.  Presumably that should be meaningful
     * to the application using the flow control classes
     */
    public void setResult_data(String result_data) {
        this.result_data = result_data;
    }

    protected String result_data = null;
    protected String error_code = null;
    protected String error_reason = null;
    protected Boolean process_result = false;
    protected Long process_time = 0L;

}
