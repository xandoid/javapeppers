package com.codeondemand.javapeppers.habanero.util.process;

import java.util.Observable;
import java.util.Observer;
import java.util.Properties;


import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public abstract class ProcessLauncher extends Observable implements Observer {

	/**
	 * This method should initializes the process launcher by locating and 
	 * parsing the XML property file as well as doing a few other steps: 
	 *   1) Calls the createProcess method to get the underlying process.
	 * @param propFile The name of the property file used for initializing the
	 *                 process launcher.
	 */
	protected boolean initialize(String propFile) {
		boolean retval = false;
		String pfile = propFile;
		
		logger.debug("Attempting to initialize process launcher class: "+getClass().toString());
		
		// There are several ways  for a property file to be
		// passed to this launcher.
		if( pfile == null ){
			
			// First try to find the name of the property file from
			// the environment.
			pfile = System.getProperty("process.property.file");
				
			// Next look for a property file based on the launcher class
			if (pfile == null) {
				pfile = MiscUtil.getBaseClassName(this) + ".properties";
				logger.debug("Using property file based on classname.properties");
			}else{
				logger.debug("Using property file specified by the system property: process.property.file");
			}
		}else{
			logger.debug("Initializing "+getClass().toString()+" with argument "+propFile );			
		}
		
		if( pfile != null ){
			try{
				properties = MiscUtil.loadXMLPropertiesFile(pfile);
			}catch( NullPointerException npe ){
				logger.error(npe.toString());
			}
			
			if( properties != null){
				
				logger.debug("Property file loaded: "+pfile );
				
				// Make sure that we have a uid and password if we need one
				if (properties.containsKey("needUIDPWD") ) {
					if (uid == null && (uid = properties.getProperty("uid")) == null ) {
						MiscUtil.getUID(null, "Enter userid: ");
					}
					if (pwd == null && (pwd = properties.getProperty("pwd")) == null ) {
						pwd = MiscUtil.getPWD(null, "Enter password: ");
					}
				}
				retval = true;
							
			}else{
				logger.error("Unable to load properties file to initialize process launcher");
			}
		}
		return retval;
	}
 
	
	protected boolean startProcess( Object arg){
		boolean retval = false;
		
		// Create a process.
		process = createProcess( arg );
		
		// Set the processs to be observed by this launcher
		setObserver(process);
		
		// Spawn a thread for this process and start it
		if( process != null ){
			Thread pthread = new Thread(process);
			pthread.start();
			retval = true;
		}else{
			logger.error("No process object was instantiated");
		}
		
		return retval;
	}
	
	/**
	 * This should only be accessed from the startProcess method.
	 * 
	 * @param process The Process to observe.
	 */
	private void setObserver(Process process) {
		if( process != null ){
			logger.debug("Adding observer to "+process.toString());
			process.addObserver(this);			
		}else{
			logger.error( "Not observable process has been created.");
		}
	}

	/**
	 * This method instantiates a Process Object.  The method in the ProcessLauncher
	 * class needs to be implemented to create the desired type of Process.
	 * 
	 * @return An instantiated Process object.
	 */
	protected abstract Process createProcess( Object arg);

	/**
	 * This is a convenience method for getting a userid and password from a
	 * user as the process starts. You can avoid this method by setting the
	 * protected uid and pwd variables from a any class that extends the
	 * ProcessLauncher class.
	 */
	protected void getUIDandPWD() {
		uid = MiscUtil.getUID(System.in, "Enter userid   (enter q to exit): ");
		pwd = MiscUtil.getPWD(System.in, "Enter password (enter q to exit: ");
		if (uid.equals("q") || pwd.equals("q")) {
			System.exit(1);
		}
	}

	// ***********************************************************************
	// Implementation for Observer interface
	// ***********************************************************************

	/**
	 * This update will occur when an object being monitored has changed. In
	 * this case, we are just outputting total processing time.
	 */
	public synchronized void update(Observable o, Object arg) {

		StringBuffer sbuff = new StringBuffer("Total process time:");
		long endTime = new java.util.Date().getTime();
		if( (endTime-startTime) < 1000*60 ){
			Double temp = new Double((endTime - startTime) / 1000);
			double cumtime = temp.longValue()/100;  
			sbuff.append(" "+ cumtime + " seconds ");
		}else if( endTime - startTime < (1000*3600 )){
			sbuff.append(" "+ (endTime-startTime)/(1000*3600) + " minutes ");			
		}else{
			Double temp = new Double((endTime - startTime) / 600.0);
			double cumtime = temp.longValue() / 100.0;  
			sbuff.append( " " + cumtime + " hours.");
		}
		logger.debug(sbuff.toString());
	}

	protected long startTime = new java.util.Date().getTime();
	protected Process process = null;
	protected String uid = null;
	protected String pwd = null;
	protected Properties properties = null;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ProcessLauncher");

}
