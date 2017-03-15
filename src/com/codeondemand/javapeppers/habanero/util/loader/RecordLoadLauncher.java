package com.codeondemand.javapeppers.habanero.util.loader;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import com.codeondemand.javapeppers.habanero.util.misc.UtlRunnable;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.util.LinkedList;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

public class RecordLoadLauncher extends UtlRunnable implements Observer {

    public RecordLoadLauncher() {
    }

    public void initialize(String args[]) {

        startTime = new java.util.Date().getTime();

        try {

            Properties p = null;
            String pfile = System.getProperty("loader.property.file");
            if (pfile == null) {
                // Load the property file and pull in the properties
                pfile = MiscUtil.getBaseClassName(this) + ".properties";
            }

            p = MiscUtil.loadXMLPropertiesFile(pfile);

            // -------------------------------------------------------
            // Get the uid/pwd unless passed
            // -------------------------------------------------------
            String uid = null;
            String pwd = null;

            if (args.length == 2) {
                uid = args[0];
                pwd = args[1];
            } else {
                uid = MiscUtil.getConsoleInput(System.in, "Enter userid: ");
                pwd = MiscUtil.getConsoleInputMasked(System.in, "Enter password: ");
                if (uid.equals("q") || pwd.equals("q")) {
                    System.exit(1);
                }
            }

            File srcdir = new File(p.getProperty("loader.loaddir"));
            File[] filelist = srcdir.listFiles();
            if (filelist != null) {
                orderInputFiles(filelist);
            }

            RecordLoader loader = createLoader();
            loader.initialize(fileq, p, uid, pwd);
            setObserver(loader);

        } catch (Exception fnfe) {
            System.err.println(fnfe.toString());
            fnfe.printStackTrace();
        }
    }

    protected RecordLoader createLoader() {
        return new RecordLoader();
    }

    protected void setObserver(Observable o) {
        o.addObserver(this);
    }

    protected void orderInputFiles(File[] filelist) {
        fileq = new LinkedList<RecordFile>();
        for (File aFilelist : filelist) {
            fileq.add(new RecordFile(aFilelist.getAbsolutePath()));
        }
    }

    // ***********************************************************************
    // Implementation for Observer interface
    // ***********************************************************************

    /**
     * This update will occur when an object being monitored has changed. In
     * this case, we are monitoring one or more EventLoader object. This method
     * is called with an argument indicating the number of events that were
     * processed by the EventLoader.
     */
    public synchronized void update(Observable o, Object arg) {
        total_count = ((Long) arg).intValue();
        endTime = new java.util.Date().getTime();
        Double temp = new Double((endTime - startTime) / 600.0);
        double cumtime = temp.longValue() / 100.0;

        logger.info("Total records processed: " + total_count);
        logger.info("\tCumulative load speed=" + 60 * (total_count / cumtime) + " records/hour.");
    }

    public static void main(String args[]) {
        RecordLoadLauncher launcher = new RecordLoadLauncher();
        launcher.initialize(args);
    }

    protected LinkedList<RecordFile> fileq = null;

    private int total_count = 0;

    private long startTime = 0;
    private long endTime = 0;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RecordLoadLauncher");

}
