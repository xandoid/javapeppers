package com.codeondemand.javapeppers.aleppo.action;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;


public class ExecuteDB2Procedure extends ExecuteDBProcedure {

	@Override
	protected boolean setParameters() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean executeProcedure() {
		// TODO Auto-generated method stub
		return false;
	}

	@SuppressWarnings("unused")
	@Override
	public boolean doInitialization() {
		boolean retval = true;
		if (props != null) {
			String keypfx = "";
			logger.debug("No properties for initialization");
			if (pmap.containsKey("keypfx")) {
				keypfx = (String) pmap.get("keypfx");
				logger.debug("Using keypfx: " + keypfx
						+ " for accessing properties db.url,db.uid,db.query, and db.driver");
			}
			String dburl = props.getProperty(keypfx + "db.url"); //$NON-NLS-1$
			String dbuser = props.getProperty(keypfx + "db.uid"); //$NON-NLS-1$
			String dbpwd = decryptPWD(props.getProperty(keypfx + "db.pwd")); //$NON-NLS-1$

			proc = props.getProperty(keypfx + "db.proc"); 
			String driver = props.getProperty(keypfx + "db.driver"); //$NON-NLS-1$
			DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);
			dbmgr.registerDriver(driver);

			if (dbmgr != null) {
				con = dbmgr.getConnection();
				if( con == null){
					logger.error("Unable to initialize connection");
				}
				try {
					String sql = props.getProperty("db.query");
					sql = "{call SYSIBMADM.SLEEP(1)}";
					if( con != null){
						stmt = con.prepareCall(sql);						
					}
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				logger.debug("Unable to initialize db manager");
			}
		} else {
			logger.error("No properties available for initializing reader.");
			retval = false;
		}
		return retval;		
	}

	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ExecuteDB2Procedure");
}
