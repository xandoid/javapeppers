package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import org.apache.logging.log4j.LogManager;

import java.io.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * This component launches a process and captures the output of standard out and
 * standard error to be passed along in the processing flow. The data flowing to
 * standard error will be combined with the data flowing to standard out in the
 * captured output unless otherwise specified. Note: You can suppress the
 * capturing of the standard outputs using configuration parameters below.
 * <p>
 * The expectation is that the process that is launched should return a zero
 * exit code if it was successful, or a parameter called "exit.code.none" should
 * be specified in the nodes configuration file to suppress the exit code check.
 * <p>
 * Parameters that are expected to be found in the configuration file for this
 * action element are (in addition to class)
 * <p>
 * command - specifies the command to be executed. args - A list of arguments to
 * be passed to the command. Separate multiple arguments with a '|' symbol.
 * capture_stdout = true or false (defaults to true) capture_stderr = true or
 * false (defaults to true) combine_stderr = true or false (defaults to true)
 * exit_code.none = if present, the flow will not test the exit code.
 *
 * @author gfa
 */
public class ShellProcess extends RecordProcessor {

    @Override
    public boolean doInitialization() {
        boolean retval = false;
        if (pmap.containsKey("command")) {

            // Create a list for adding the commands and arguments
            ArrayList<String> args = new ArrayList<>();

            // Put the basic command as the first argument in the list.
            // Save for logging purposes.
            command = (String) pmap.get("command");
            logger.debug("Command:" + command);
            args.add(command);

            // Tokenize and add any additional arguments to the argument
            // list.
            if (pmap.containsKey("args")) {
                String temp = (String) pmap.get("args");
                if (temp.length() > 0) {
                    StringTokenizer st = new StringTokenizer(temp, "|");
                    while (st.hasMoreTokens()) {
                        String tok = st.nextToken();
                        args.add(tok);
                        logger.debug("argument:" + tok);
                    }
                }
            }

            // The "outputField" parameter should reference a DataCapsule in the
            // record that has the information that needs to be pushed to the
            // std
            // input of the command.
            if (pmap.containsKey("outputField")) {
                outputField = (String) pmap.get("outputField");
            }

            foo = new ProcessBuilder(args);
            retval = true;
        }
        return retval;
    }

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {
        RecordCapsule retval = input;
        try {

            // Redirect error stream unless 'combine_stderr' is set to false
            if (pmap.containsKey("combine_stderr")) {
                combine_stderr = Boolean.parseBoolean((String) pmap.get("combine_stderr"));
            }
            foo.redirectErrorStream(combine_stderr);

            // Hook up the streams.
            if (pmap.containsKey("capture_stdout")) {
                capture_stdout = Boolean.parseBoolean((String) pmap.get("capture_stdout"));
            }
            if (pmap.containsKey("capture_stderr")) {
                capture_stderr = Boolean.parseBoolean((String) pmap.get("capture_stderr"));
            }

            BufferedReader sr = null;
            BufferedReader er = null;
            StringBuffer sb = null;
            StringBuffer eb = null;
            int linecount = 1;

            Process bar = foo.start();

            // Ship the contents of the outputField to the subprocess.
            if (outputField != null) {
                if (input.getField(outputField) != null && !input.getField(outputField).isNull()) {
                    try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(bar.getOutputStream()))) {
                        br.write(input.getField(outputField).getData().toString());
                        br.flush();
                        br.close();
                    }
                }
            }

            Thread.sleep(sleep_time);

            bar.waitFor();

            if (capture_stdout) {
                sr = new BufferedReader(new InputStreamReader(bar.getInputStream()));
                sb = new StringBuffer();
            }

            if (capture_stderr && !combine_stderr) {
                er = new BufferedReader(new InputStreamReader(bar.getErrorStream()));
                eb = new StringBuffer();
            }

            while (sr != null && sr.ready()) {
                if (sr.ready()) {
                    sb.append(sr.readLine() + "\n");

                    System.out.print(" " + linecount++);
                    if (!sr.ready()) {
                        Thread.sleep(sleep_time);
                    }
                }
                Thread.yield();
            }

            while (er != null && er.ready()) {
                if (er.ready()) {
                    eb.append(er.readLine() + "\n");
                    if (!er.ready()) {
                        Thread.sleep(sleep_time);
                    }
                }
                Thread.yield();
            }

            if (sr != null) {
                input.addDataCapsule(new DataCapsule(command + ":output", sb.toString()), false);
                sr.close();
            }

            if (er != null) {
                input.addDataCapsule(new DataCapsule(command + ":error", eb.toString()), false);
                er.close();
            }

            // Get the exit value, if it is non-zero, then there was an error
            // and
            // we should not continue.
            int temp = bar.exitValue();
            if (temp != 0 && !pmap.containsKey("exit_code.none")) {
                retval = null;
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            retval = null;
        } catch (InterruptedException e) {

            retval = null;
        }

        return retval;
    }

    private String outputField = null;
    private ProcessBuilder foo = null;
    private String command = null;
    private boolean combine_stderr = true;
    private boolean capture_stdout = true;
    private boolean capture_stderr = true;

    private final static int sleep_time = 10;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ShellProcess");

    public void done() {
        // TODO Auto-generated method stub

    }

}
