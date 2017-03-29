/**
 *
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.reader.SourceReader;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The FlowControlReader provides the processing token (a RecordCapsule) that
 * is used as the data context for a flow process.  It depends on the existence
 * of three database control and metadata tables.  These names of these tables
 * can be specified in the configuration file, however the definitions of the
 * table is must be in accordance with the standard aleppo.dataflow package
 * approach.
 * <p>
 * The logic used by this reader is as follows:
 * <p>
 * 1) It reads the 'trigger' table (usually called FLOW_TRIGGER to determine
 * if there are any unprocessed flows.  This will be indicated by the
 * value of 'N' or 'E' in the STATUS column of the 'trigger' table.
 * 2) Each unprocessed flow is handled sequentially, although typically there
 * will be just one unprocessed flows.
 * 3) For each flow, two DataCapsules are added to the processing token.  They
 * have names 'FLOW_ID' and 'CFG_ID' and these will be passed along to the
 * processing steps for checking to see if each step needs to be processed.
 *
 * @author gfa
 */
public class FlowControlReader extends SourceReader {

    @Override
    public boolean close() {
        try {
            if (stmt != null) {
                stmt.close();
                if (rs != null) {
                    rs.close();
                }
            }

        } catch (SQLException e) {
            logger.error(e.toString());
        }
        return true;
    }

    @Override
    public Object read() {
        RecordCapsule rc = null;
        try {
            if (rs != null && rs.next()) {
                long flow_id = rs.getLong("FLOW_ID");
                int cfg_id = rs.getInt("CFG_ID");
                rc = new RecordCapsule("record_" + flow_id + "_" + cfg_id, null);
                rc.addDataCapsule(new DataCapsule("FLOW_ID", flow_id), false);
                rc.addDataCapsule(new DataCapsule("CFG_ID", cfg_id), false);

                // Mark the start time.
                setFlowTime(flow_id, cfg_id);

            } else {
                logger.debug("ResultSet is empty:" + rs);
            }
        } catch (SQLException e) {
            logger.error(e.toString());
        }
        return rc;
    }

    @Override
    public boolean reset() {
        try {
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            logger.error(e.toString());
        }
        rs = getFlows();
        return false;
    }

    @Override
    public boolean doInitialization() {
        boolean retval = false;

        if (props == null && pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE)) {
            props = MiscUtil.loadXMLPropertiesFile((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE));
        }
        if (props != null) {
            c = buildDBConnection();
            if (c != null) {
                logger.debug("Database connection:" + c.toString());
                rs = getFlows();
                retval = true;
            }
        } else {
            logger.error("No properties available for FlowControlReader");
        }

        return retval;
    }

    //
    // Sets the starting time for the process flow specified by flow_id and cfg_id
    //
    private void setFlowTime(long flow_id, int cfg_id) {
        try {
            stmt = c.createStatement();
            String schema = props.getProperty("db.etl.schema");
            String trigger_table = props.getProperty("db.table.flow_trigger");
            logger.debug("updating flow times.");

            // Set the time and status to 'I'n progress
            stmt.executeUpdate("Update " + schema + "." + trigger_table + " set (status,start_ts)=('I',current timestamp) where flow_id=" + flow_id + " and cfg_id=" + cfg_id);
            c.commit();
        } catch (SQLException e) {
            logger.error(e.toString());
        }
    }

    // Fetches the flow_id and cfg_id for any unfinished (or unstarted ) process flows.
    private ResultSet getFlows() {
        ResultSet retval = null;
        try {
            stmt = c.createStatement();
            String schema = props.getProperty("db.etl.schema");
            String trigger_table = props.getProperty("db.table.flow_trigger");
            logger.debug("Reading dataflow steps.");
            retval = stmt.executeQuery("Select FLOW_ID,CFG_ID from " + schema + "." + trigger_table + " where STATUS in ('N','E','I')");

        } catch (SQLException e) {
            logger.error(e.toString());
        }

        return retval;
    }

    private Connection buildDBConnection() {
        Connection retval = null;

        String dburl = props.getProperty("db.url"); //$NON-NLS-1$
        String dbuser = props.getProperty("db.uid"); //$NON-NLS-1$
        String dbpwd = null;
        if (props.getProperty("db.encrypted.pwd") != null && props.getProperty("db.encrypted.pwd").equals("true")) {
            dbpwd = MiscUtil.decodeB64String(props.getProperty("db.pwd")); //$NON-NLS-1$
        } else {
            dbpwd = props.getProperty("db.pwd"); //$NON-NLS-1$
        }
        String driver = props.getProperty("db.driver"); //$NON-NLS-1$

        DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);
        dbmgr.registerDriver(driver);

        retval = dbmgr.getConnection(dburl, dbuser, dbpwd);

        if (retval != null) {
            logger.debug("Created connection:" + retval);
        } else {
            logger.error("Unable to create database connection.");
        }

        return retval;
    }

    ResultSet rs = null;
    Connection c = null;
    Statement stmt = null;

    Statement pstmt = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowControlReader");
    // Properties props = null;
}
