/**
 *
 */
package com.codeondemand.javapeppers.aleppo.flowcontrol;

import com.codeondemand.javapeppers.aleppo.action.AleppoRunnable;
import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.util.Properties;

/**
 * The FlowProcessCheck provides the functionality to determine if all of the
 * steps for the particular process flows were successfully handled. It does
 * that by checking the flow status table (typically this is named FLOW_STATUS)
 * for any steps that are either missing or ended with an error. By counting the
 * number of steps in the 'status' table that have the specified FLOW_ID/CFG_ID
 * combination and determining if there are the same number of steps that have a
 * status of 'S' for success.
 *
 * @author gfa
 */
public class FlowProcessCheck extends AleppoRunnable {

    @Override
    public void run() {

        if (processData != null && (props = (Properties) processData.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY)) != null) {
            c = buildDBConnection();

            flow_id = Long.parseLong(processData.getField("FLOW_ID").getData().toString());
            cfg_id = Integer.parseInt(processData.getField("CFG_ID").getData().toString());
            result.setProcess_result(checkResults());
        } else {
            logger.error("No properties object available");
        }
        setChanged();
        notifyObservers(result);
    }

    private boolean checkResults() {
        boolean retval = false;
        int okcount = 0;
        int stepcount = 0;
        int oldstep = 0;
        String db_schema = props.getProperty("db.etl.schema"); //$NON-NLS-1$
        String step_table = props.getProperty("db.table.flow_status"); //$NON-NLS-1$
        String trigger_table = props.getProperty("db.table.flow_trigger"); //$NON-NLS-1$
        addNewTrigger = Boolean.parseBoolean(props.getProperty("flow_add_trigger", "false"));

        String sqlString = "Select step_number,step_status from " + db_schema + "." + step_table + " where FLOW_ID = " + flow_id + " and CFG_ID =" + cfg_id + " order by step_number";
        try (Statement stmt = c.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sqlString)) {

                // While processing the result set, only count each step number
                // once. Count
                // each row that hasa a STATUS equal to 'S' since there will
                // only be one 'S'
                // entry for any given step number. The process was successful
                // if the total
                // success count equals the distinct number of steps.
                while (rs.next()) {
                    int step = rs.getInt(1);
                    if (step != oldstep) {
                        stepcount++;
                        oldstep = step;
                    }
                    String code = rs.getString(2);
                    if (code.equals("S")) {
                        okcount++;
                    }
                }
                rs.close();
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error(e.toString());
        }

        logger.debug("result check: steps=" + stepcount + "  okcount=" + okcount);

        if ((okcount + stepcount) > 0) {
            // Update the flow status
            String pString = "Update " + db_schema + "." + trigger_table + " set (status,end_ts) = (?,?) where flow_id = ? and" + " cfg_id = ?";
            try (PreparedStatement pstmt = c.prepareStatement(pString)) {
                if (stepcount == okcount) {
                    pstmt.setString(1, "C");
                } else {
                    pstmt.setString(1, "E");
                }
                pstmt.setTimestamp(2, DbUtil.currentTimestamp());
                pstmt.setLong(3, flow_id);
                pstmt.setInt(4, cfg_id);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.toString());
            }

            if (stepcount == okcount && addNewTrigger) {
                String pString2 = "Insert into " + db_schema + "." + trigger_table + " (CFG_ID,STATUS,ATTEMPT_COUNT,START_TS,END_TS) VALUES(?,'N',0,null,null)";
                flow_id++;
                try (PreparedStatement pstmt2 = c.prepareStatement(pString2)) {
                    pstmt2.setInt(1, cfg_id);
                    pstmt2.executeUpdate();
                } catch (SQLException e) {
                    logger.error(e.toString());
                }
            }
        }
        retval = true;
        return retval;
    }

    private Connection buildDBConnection() {
        Connection retval = null;

        String dburl = props.getProperty("db.url"); //$NON-NLS-1$
        String dbuser = props.getProperty("db.uid"); //$NON-NLS-1$
        String dbpwd = null;
        if (props.getProperty("db.encrypted.pwd").equals("true")) {
            dbpwd = MiscUtil.decodeB64String(props.getProperty("db.pwd")); //$NON-NLS-1$
        } else {
            dbpwd = props.getProperty("db.pwd"); //$NON-NLS-1$
        }
        String driver = props.getProperty("db.driver"); //$NON-NLS-1$

        DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);
        dbmgr.registerDriver(driver);

        retval = dbmgr.getConnection(dburl);
        return retval;
    }

    Connection c = null;
    PreparedStatement pstmt = null;
    private long flow_id = 0L;
    private int cfg_id = 0;
    private boolean addNewTrigger = false;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowProcessCheck");
    Properties props = null;
    protected FlowResult result = new FlowResult();
}
