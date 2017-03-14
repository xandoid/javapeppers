/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.action.AleppoRunnable;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public abstract class FlowProcessStep extends AleppoRunnable {

	@Override
	public void run() {
		long startTime = new GregorianCalendar().getTimeInMillis();
		if (processData != null) {
			// Pull in a properties file if one is available.
			props = (Properties) processData.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY);

			String classname = this.getClass().getSimpleName();
			if (props != null) {
				if ((con = buildDBConnection()) != null && createStatements() && (pmap.get("stepname") != null)) {
					Timestamp start = DbUtil.currentTimestamp();
					String stepname = (String) pmap.get("stepname");
					int stepnum = getStepNumber(stepname);
					int trycount = getTryCount(stepnum);

					//
					// Only proceed if we have valid step number and try count
					//
					if (stepnum > 0 && trycount > 0) {
						doRun();
						result.setProcess_result(isSuccessfulRun());
						updateStatus(stepnum, trycount, start);
						if (result.process_result) {
							if (result.getResult_data() == null) {
								result.setResult_data("SUCCESS");
							}
						} else {
							if (result.getResult_data() == null) {
								result.setResult_data("FAILED");
							}
						}
						processData.addDataCapsule(new DataCapsule(classname, result), false);
					} else {
						if (stepnum > 0) {
							result.setProcess_result(true);
							result.setResult_data("SKIPPED");
							processData.addDataCapsule(new DataCapsule(classname, result), false);
						} else {
							result.setProcess_result(false);
							result.setResult_data("STEP NOT FOUND");
							processData.addDataCapsule(new DataCapsule(classname, result), false);
						}
					}
				} else {
					logger.error("Unable to initialize database artifacts.");
					result.setResult_data("UNABLE TO RUN STEP");
					result.setProcess_result(false);
				}
			} else {
				logger.error("No properties provided to this process node");
				result.setResult_data("UNABLE TO RUN STEP");
				result.setProcess_result(false);
			}
		} else {
			logger.error("Process data object is null");
		}
		long endTime = new GregorianCalendar().getTimeInMillis();
		result.setProcess_time(endTime - startTime);

		notify(result);
		setChanged();
		notifyObservers(result);

	}

	/**
	 * Subclasses should do any application specific tasks that they desire
	 * based on the result.
	 * 
	 * @param res
	 *            The FlowResult object for this task.
	 */
	protected void notify(FlowResult res) {
		// Default behavior is to do nothing in this class.
		logger.debug("Calling noaction notify method");
	}

	private boolean updateStatus(int step, int trycnt, Timestamp start) {
		boolean retval = false;
		try {

			insertStepStatusStmt.setInt(1, Integer.parseInt(processData.getField("FLOW_ID").getData().toString()));
			insertStepStatusStmt.setInt(2, Integer.parseInt(processData.getField("CFG_ID").getData().toString()));

			insertStepStatusStmt.setInt(3, step);
			insertStepStatusStmt.setInt(4, trycnt);

			if (result.getProcess_result()) {
				insertStepStatusStmt.setString(5, "S");
			} else {
				insertStepStatusStmt.setString(5, "E");
			}

			if (result.getResult_data() != null) {
				String temp = result.getResult_data().toString().trim();
				if (temp.startsWith("<")) {
					insertStepStatusStmt.setString(6, temp);
				} else {
					insertStepStatusStmt.setString(6, "<RESULT message=\"" + temp + "\" />");
				}
			} else {
				insertStepStatusStmt.setString(6, "<RESULT message=\"no result msg\" />");
			}

			insertStepStatusStmt.setTimestamp(7, start);
			insertStepStatusStmt.executeUpdate();
			retval = true;

		} catch (SQLException e) {
			logger.error(e.toString());
		}

		return retval;
	}

	private boolean createStatements() {
		boolean retval = true;

		try {
			String schema = props.getProperty("db.etl.schema"); //$NON-NLS-1$
			String stat_table = props.getProperty("db.table.flow_status");
			String step_table = props.getProperty("db.table.flow_cfg");

			// Create a statement for inserting the result of the processing
			// step.
			String sql = "insert into " + schema + "." + stat_table
					+ "(FLOW_ID,CFG_ID,STEP_NUMBER,TRY_NUMBER,STEP_STATUS," + " STATUS_DETAIL,START_TS,END_TS) "
					+ "values(?,?,?,?,?,?,?,CURRENT_TIMESTAMP)";
			insertStepStatusStmt = con.prepareStatement(sql);

			String query = "Select STEP_NUMBER from " + schema + "." + step_table + " where STEP_NAME=? and CFG_ID=?";
			stepNumberSelectStmt = con.prepareStatement(query);

			String query3 = "Select STEP_STATUS,TRY_NUMBER from " + schema + "." + stat_table
					+ " where STEP_NUMBER=? and FLOW_ID=? and CFG_ID=?" + " order by TRY_NUMBER asc";
			selectStepStatusStmt = con.prepareStatement(query3);

		} catch (SQLException e) {
			logger.error(e.toString());
			retval = false;
		}
		return retval;
	}

	private int getTryCount(int step) {
		int trycnt = 1;
		if (step > 0) {
			try {
				selectStepStatusStmt.setInt(1, step);
				selectStepStatusStmt.setInt(2, Integer.parseInt(processData.getField("FLOW_ID").getData().toString()));
				selectStepStatusStmt.setInt(3, Integer.parseInt(processData.getField("CFG_ID").getData().toString()));
				try (ResultSet rs = selectStepStatusStmt.executeQuery()) {

					int temp = 0;
					while (rs.next()) {
						temp = rs.getInt(2);
						if (rs.getString(1).equals("S")) {
							trycnt = -1;
							break;
						} else {
							trycnt = temp + 1;
						}
					}
					rs.close();
				}
				selectStepStatusStmt.close();

			} catch (SQLException e) {
				logger.error(e.toString());
			}
		} else {
			logger.error("Error in getTryCount: Step must be a non-zero positive integer");
		}
		return trycnt;
	}

	// Getting the step number.
	private int getStepNumber(String name) {
		int stepnum = -1;
		try {

			// Get the step number from the table for this step.
			stepNumberSelectStmt.setString(1, name);
			int cfg = Integer.parseInt(processData.getField("CFG_ID").getData().toString());
			stepNumberSelectStmt.setInt(2, cfg);
			try (ResultSet rs = stepNumberSelectStmt.executeQuery()) {

				if (rs.next()) {
					stepnum = rs.getInt(1);
				}
				rs.close();
			}
			stepNumberSelectStmt.close();

		} catch (SQLException e) {
			logger.error(e.toString());
		}

		return stepnum;
	}

	protected Connection buildDBConnection() {
		Connection retval = null;

		String dburl = props.getProperty("db.url"); //$NON-NLS-1$
		String dbuser = props.getProperty("db.uid"); //$NON-NLS-1$
		logger.debug("url:" + dbuser);
		logger.debug("uid:" + dbuser);
		String dbpwd = null;

		// DO NOT EXPOSE UNENCRYPTED PASSWORD TO THE LOG FILE.
		if (props.getProperty("db.encrypted.pwd").equals("true")) {
			dbpwd = MiscUtil.decodeB64String(props.getProperty("db.pwd")); //$NON-NLS-1$
			logger.debug("pwd (encrypted: " + props.getProperty("db.pwd"));
		} else {
			dbpwd = props.getProperty("db.pwd"); //$NON-NLS-1$
			logger.debug("pwd:" + dbpwd);
		}

		String driver = props.getProperty("db.driver"); //$NON-NLS-1$

		DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);
		dbmgr.registerDriver(driver);

		retval = dbmgr.getConnection(dburl, dbuser, dbpwd);
		return retval;
	}

	protected abstract void doRun();

	protected abstract boolean isSuccessfulRun();

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowProcessStep");

	protected Properties props = null;
	private Connection con = null;
	private PreparedStatement insertStepStatusStmt = null;
	private PreparedStatement stepNumberSelectStmt = null;
	private PreparedStatement selectStepStatusStmt = null;

	protected FlowResult result = new FlowResult();
}
