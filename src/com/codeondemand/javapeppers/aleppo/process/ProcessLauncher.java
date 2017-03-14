/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.process;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Observable;
import java.util.Observer;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.connector.RecordConnector;

/**
 * ProcessLauncher is the the process control hub for Aleppo dataflow processes,
 * although it really just invokes an instance of ConfigurationLoader and then
 * runs each of the RecordConnectors in as separate thread in parallel. Since
 * each of these threads is running under the same JVM, you need to be aware of
 * memory issues.
 * 
 * Note: Since each of the connector blocks parsed by the ConfigurationLoader is
 * run in parallel, you need to be aware of any processing dependencies between
 * them. Typically you will only run totally independent processes in parallel.
 * 
 * @author gfa
 * 
 */
public class ProcessLauncher implements Observer {

	public boolean process(ArrayList<RecordConnector> c) {
		success = false;
		System.err.println(new GregorianCalendar().getTime());
		// Allocate space for enough threads to run
		// all of the connectors.
		if (c != null) {

			Thread[] threads = new Thread[c.size()];
			for (int i = 0; i < c.size(); i++) {
				RecordConnector r = c.get(i);
				r.addObserver(this);
				Thread t = new Thread(r);
				threads[i] = t;
				logger.debug("Starting thread for connector: " + t);
				t.start();
			}

			// Join each of the threads so that this thread will
			// wait for all of the processes to finish before
			// it completes. It is unimportant in which order
			// the Threads finish, since this main thread will
			// not exit until each thread in the collection is
			// finished.
			try {
				for (int i = 0; i < c.size(); i++) {
					if (threads[i].isAlive()) {
						threads[i].join();
					}
				}
				success = true;
			} catch (InterruptedException e) {
				e.printStackTrace();
				success = false;
			}
		}
		System.err.println(new GregorianCalendar().getTime());
		return success;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {


		if (args.length == 1) {
			ConfigurationLoader loader = new ConfigurationLoader();
			ArrayList<RecordConnector> foo = loader.initialize(args[0]);
			ProcessLauncher bar = new ProcessLauncher();
			logger.debug("Processing successful: " + bar.process(foo));
		}
	}

	public void update(Observable o, Object arg) {
		if (o instanceof RecordConnector && arg instanceof Long) {
			// If the return value is negative, that indicates that
			// less than the minimum required records were moved.
			if ((Long) arg < 0L) {
				success = false;
			}
			logger.debug("Connector reports delivering " + (Long) arg
					+ " record.");
		}
	}

	private boolean success = true;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ProcessLauncher");
}
