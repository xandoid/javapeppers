/**
 *
 */
package com.codeondemand.javapeppers.aleppo.writer;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.util.TreeMap;

/**
 * Is used to write incoming data that is in a RecordCapsule object into a
 * database table. It expects that their will be some properties specified that
 * specify connection and database driver information, etc. The properties will
 * be set using normal Aleppo techniques (which includes making available all
 * the System properties to the components, as well as any properties loaded by
 * the flow system. The properties that need to be specified are optionally
 * prefixed by a string that is given by the keypfx parameter that will be in
 * the configuration block for the DBRecordWriter component.
 * <p>
 * The expected properties are (if keypfx is test.):
 * <p>
 * <ul>
 * <li>test.db.url = The jdbc connection url for the database
 * <li>test.db.uid = The user id for making the connection.
 * <li>test.db.pwd = The password (usually encrypted) for making the connection
 * <li>test.db.encrypted.pwd = true or false indicating test.db.pwd is encrypted
 * <li>test.db.schema = The schema to use for the connection and statements
 * <li>test.db.table = The target table for the data.
 * <li>test.db.driver = the jdbc driver class to register.
 * </ul>
 *
 * @author gfa
 */
public class DBRecordWriter extends DestinationWriter {

    public boolean close() {
        boolean retval = false;
        if (con != null) {
            try {
                if (!con.getAutoCommit()) {
                    con.commit();
                }
                con.close();
                con = null;
                retval = true;
            } catch (SQLException e) {
                logger.error(e.toString());
                e.printStackTrace();
            }
        }
        return retval;
    }

    public boolean reset() {
        close();
        return doInitialization();
    }

    public boolean write(Object data) {
        if (!initialized) {
            initialized = doInitialization();
        }
        boolean retval = false;
        if (data != null) {
            if (insertsql == null) {
                if (buildInsertSQL(data)) {
                    retval = writeRecord(data);
                }
            } else {
                retval = writeRecord(data);
            }
        }

        return retval;
    }

    protected boolean writeRecord(Object record) {
        boolean retval = false;
        if (record instanceof RecordCapsule) {
            RecordCapsule rc = (RecordCapsule) record;
            int count = rc.getFieldCount();

            Object bar = null;

            for (int i = 0; i < count; i++) {
                bar = rc.getField(i).getData();
                try {
                    pstmt.setObject(i + 1, bar);
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            try {
                pstmt.execute();
                retval = true;
            } catch (SQLException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    /*
     * This dynamically builds a prepared statement such that it can be used as
     * an insert statement for the table.
     *
     * @param data This really needs to be a RecordCapsule object.
     */
    protected boolean buildInsertSQL(Object data) {
        boolean retval = false;
        if (!initialized) {
            initialized = doInitialization();
        }
        if (data instanceof RecordCapsule) {
            RecordCapsule rc = (RecordCapsule) data;
            StringBuffer sbuff = new StringBuffer("insert into " + //$NON-NLS-1$
                    dbschema + "." + dbtable + "( "); //$NON-NLS-1$ //$NON-NLS-2$
            types = new int[rc.getFieldCount()];
            String comma = new String();
            for (int idx = 0; idx < rc.getFieldCount(); idx++) {
                String name = rc.getField(idx).getName();
                //System.out.println(name);
                types[idx] = typeTable.get(name).intValue();
                sbuff.append(comma + name);
                comma = ","; //$NON-NLS-1$
            }
            sbuff.append(") values( "); //$NON-NLS-1$
            comma = new String();
            String quote = new String();
            for (int i = 0; i < rc.getFieldCount(); i++) {
                quote = new String();
                sbuff.append(comma);
                if (types[i] == java.sql.Types.CHAR || types[i] == java.sql.Types.VARCHAR || types[i] == java.sql.Types.DATE || types[i] == java.sql.Types.TIMESTAMP || types[i] == java.sql.Types.LONGVARCHAR || types[i] == java.sql.Types.OTHER) {

                    // Not using quotes anymore since using prepared statements
                    quote = ""; //$NON-NLS-1$

                }
                sbuff.append(quote + "?" + quote); //$NON-NLS-1$
                comma = ","; //$NON-NLS-1$
            }
            sbuff.append(")"); //$NON-NLS-1$

            try {
                insertsql = sbuff.toString();
                pstmt = con.prepareStatement(insertsql);
            } catch (SQLException e) {
                logger.error(e.toString());
            }
            retval = true;
        }

        return retval;
    }

    /**
     * Opens the target table to determine the type of each of the columns in
     * the target table. This also creates a connection to the database which is
     * left open for use by the insert transactions.
     *
     * @return true if successfully initialized
     */
    public boolean doInitialization() {
        boolean retval = false;

        if (pmap.containsKey("keypfx")) {
            keypfx = pmap.get("keypfx").toString();
        }

        String dburl = props.getProperty(keypfx + "db.url"); //$NON-NLS-1$
        String dbuser = props.getProperty(keypfx + "db.uid"); //$NON-NLS-1$
        String dbpwd = props.getProperty(keypfx + "db.pwd"); //$NON-NLS-1$

        if (dbpwd != null && props.containsKey(keypfx + "db.encrypted.pwd")) {
            if (Boolean.parseBoolean(props.getProperty(keypfx + "db.encrypted.pwd", "").toString())) {
                dbpwd = MiscUtil.decodeB64String(dbpwd);
            }
        }

        dbtable = props.getProperty(keypfx + "db.table"); //$NON-NLS-1$
        dbschema = props.getProperty(keypfx + "db.schema"); //$NON-NLS-1$
        String driver = props.getProperty(keypfx + "db.driver"); //$NON-NLS-1$

        // Register the database driver.
        DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);

        dbmgr.registerDriver(driver);

        if (dbmgr != null) {
            con = dbmgr.getConnection();

            if (con != null) {

                try (Statement stmt = con.createStatement()) {
                    // System.out.println( "stmt->"+stmt.toString());
                    if (stmt != null) {
                        if (props.containsKey(keypfx + "db.query")) {
                            insertsql = props.getProperty(keypfx + "db.query");
                            logger.debug(insertsql);
                            //System.out.println(insertsql);
                            retval = true;
                            try {
                                pstmt = con.prepareStatement(insertsql);
                            } catch (SQLException e) {
                                logger.error(e.toString());
                            }
                        } else {
                            retval = stmt.execute("Select * from " + dbschema + "." + dbtable + //$NON-NLS-1$ //$NON-NLS-2$
                                    " fetch first row only"); //$NON-NLS-1$
                            try (ResultSet rs = stmt.getResultSet()) {
                                if (rs != null) {
                                    ResultSetMetaData rsmd = rs.getMetaData();
                                    typeTable = DbUtil.buildTypeTable(rsmd);
                                    if (typeTable != null) {
                                        retval = true;
                                    }

                                    // clean up the resources
                                    rs.close();
                                }
                                stmt.close();
                            }
                        }
                    } else {
                        logger.error("Unable to create a statement");
                    }
                } catch (SQLException e) {
                    logger.error(e.toString());
                }

            } else {
                logger.error("Unable to create connection to database.");
            }

        }

        return retval;
    }

    /**
     * Allows fetching the keypfx parameter in the event that this class is used
     * outside of the Aleppo framework. This is not a recommended practice, but
     * is enabled.
     *
     * @return The String object containing the current keypfx value.
     */
    public String getKeypfx() {
        return keypfx;
    }

    /**
     * Allows setting the keypfx parameter in the event that this class is used
     * outside of the Aleppo framework. This is not a recommended practice, but
     * is enabled.
     *
     * @param keypfx The keypfx value to use. This must be set before any write
     *               operations are attempted.
     */
    public void setKeypfx(String keypfx) {
        this.keypfx = keypfx;
    }

    protected TreeMap<String, Integer> typeTable = null;

    /**
     * This variable will be initialized to the name of the table that is the
     * target of the writes.
     */
    protected String dbtable = null;

    /**
     * This variable will be initialized to the name of the schema that should
     * be used with the target table.
     */
    protected String dbschema = null;

    /**
     * This variable will have the open connection to the database
     */
    protected Connection con = null;

    protected int[] types = null;
    protected String keypfx = "";
    protected boolean initialized = false;

    protected String insertsql = null;
    protected PreparedStatement pstmt = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DBRecordWriter");

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

}
