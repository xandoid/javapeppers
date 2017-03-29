/**
 *
 */
package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.util.TreeMap;

/**
 * This class is an implementation of SourceReader in the context of a database
 * read.
 *
 * @author gfa
 */
public class DBSourceReader extends SourceReader {

    public boolean close() {
        try {
            stmt.cancel();
            if (!stmt.isClosed()) {
                stmt.close();
            }
            rs = null;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Object read() {
        RecordCapsule retval = null;
        try {
            if (rs != null && rs.next()) {
                retval = newRecord("record" + row);

                for (int i = 1; i <= fieldcount; i++) {
                    String name = rsmd.getColumnLabel(i);
                    int type = getType(name);

                    Object bar = null;
                    if (type == java.sql.Types.OTHER) {
                        bar = rs.getString(i);
                    } else {
                        bar = rs.getObject(i);
                    }
                    if (bar == null) {
                        bar = new String();
                    }
                    addField(retval, name, type, bar);
                }
                endRecord(retval);
                row++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQL Exception while building record " + e.getLocalizedMessage());
        }

        return retval;
    }

    /**
     * Returns the type (java.sql.Types) of the data that is associated with a
     * named field in the record.
     *
     * @param field The name of the field. Usually derived from the header.
     * @return The type of the data in the field.
     */
    protected int getType(String field) {
        int retval = java.sql.Types.NULL;
        if (field != null) {
            if (getTypeTable().containsKey(field)) {
                retval = getTypeTable().get(field);
            } else {
                logger.error(AleppoMessages.getString("DBSourceReader.2")); //$NON-NLS-1$
            }
        } else {
            logger.error(AleppoMessages.getString("DBSourceReader.3")); //$NON-NLS-1$
        }
        return retval;
    }

    public boolean reset() {

        try {
            stmt.close();
            rs = null;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return initialize(con, query);
    }

    public boolean initialize(Connection dbcon, String dbquery) {

        boolean retval = false;
        query = dbquery;
        con = dbcon;
        logger.debug("Initializing source with connection and query." + dbcon + ":" + dbquery);
        logger.debug("connection: " + con);
        logger.debug("query:" + query);

        if (con != null && dbquery != null) {
            try {
                stmt = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                //stmt.setFetchSize(Integer.MIN_VALUE);
                if (stmt != null) {
                    logger.debug(dbquery);
                    retval = stmt.execute(dbquery);
                    rs = stmt.getResultSet();
                    if (rs != null) {
                        rsmd = rs.getMetaData();
                        setTypeTable(DbUtil.buildTypeTable(rsmd));
                        setTypeSizeTable(DbUtil.buildTypeSizeTable(rsmd));
                        setTypeNameTable(DbUtil.buildTypeNameTable(rsmd));
                        setTypePrecisionTable(DbUtil.buildTypeScaleTable(rsmd));
                        fieldcount = rsmd.getColumnCount();

                        // Build the RecordCapsule for the header
                        recordHeader = new RecordCapsule("header", "record"); //$NON-NLS-1$
                        for (int i = 1; i <= fieldcount; i++) {
                            recordHeader.addDataCapsule(new DataCapsule(rsmd.getColumnName(i), null), false);
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error(e.toString());
            }
        } else {
            logger.error(AleppoMessages.getString("DBSourceReader.5")); //$NON-NLS-1$
        }
        return retval;
    }

    public boolean doInitialization() {
        boolean retval = false;

        if (pmap.containsKey("rowtag")) {
            rowtag = (String) pmap.get("rowtag");
        }

        if (props != null) {
            logger.debug(AleppoMessages.getString("DBSourceReader.6")); //$NON-NLS-1$
            if (pmap.containsKey("keypfx")) {
                keypfx = (String) pmap.get("keypfx");
                logger.debug("Using keypfx: " + keypfx + " for accessing properties db.url,db.uid,db.query, and db.driver");
            }
            String dburl = props.getProperty(keypfx + "db.url"); //$NON-NLS-1$
            String dbuser = props.getProperty(keypfx + "db.uid"); //$NON-NLS-1$
            String dbpwd = decryptPWD(props.getProperty(keypfx + "db.pwd")); //$NON-NLS-1$

            query = props.getProperty(keypfx + "db.query"); //$NON-NLS-1$
            String driver = props.getProperty(keypfx + "db.driver"); //$NON-NLS-1$
            DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);

            if (dbmgr != null) {
                dbmgr.registerDriver(driver);
                con = dbmgr.getConnection();

                if (con != null) {
                    logger.debug("Retrieved db connection: " + con.toString());
                    retval = initialize(con, query);
                } else {
                    logger.debug(AleppoMessages.getString("DBSourceReader.12")); //$NON-NLS-1$
                }
            }
        } else {
            logger.error("No properties available for initializing reader.");
            retval = false;
        }

        return retval;
    }

    protected boolean setTypeTable(TreeMap<String, Integer> t) {
        boolean retval = false;
        if (t != null) {
            typeTable = t;
            retval = true;
        }
        return retval;
    }

    protected boolean setTypeSizeTable(TreeMap<String, Integer> t) {
        boolean retval = false;
        if (t != null) {
            typeSizeTable = t;
            retval = true;
        }
        return retval;
    }

    protected boolean setTypePrecisionTable(TreeMap<String, Integer> t) {
        boolean retval = false;
        if (t != null) {
            typeScaleTable = t;
            retval = true;
        }
        return retval;
    }


    protected boolean setTypeNameTable(TreeMap<String, String> t) {
        boolean retval = false;
        if (t != null) {
            typeNameTable = t;
            retval = true;
        }
        return retval;
    }

    protected TreeMap<String, Integer> getTypeTable() {
        if (typeTable == null) {
            typeTable = new TreeMap<String, Integer>();
        }
        return typeTable;
    }

    protected RecordCapsule newRecord(String name) {
        return new RecordCapsule(name, null);
    }

    protected void endRecord(RecordCapsule rc) {
        //TO-DO: what should go here?
    }

    protected void addField(RecordCapsule rc, String name, int type, Object value) {
        DataCapsule d = new DataCapsule(name, value);
        d.setMetaData("type", type); //$NON-NLS-1$
        d.setMetaData("typeName", typeNameTable.get(name));
        d.setMetaData("length", typeSizeTable.get(name));
        d.setMetaData("scale", typeScaleTable.get(name));
        rc.addDataCapsule(d, false);

    }

    protected String decryptPWD(String input) {
        return input;
    }

    protected TreeMap<String, Integer> typeTable = null;
    protected TreeMap<String, String> typeNameTable = null;
    protected TreeMap<String, Integer> typeSizeTable = null;
    protected TreeMap<String, Integer> typeScaleTable = null;

    public RecordCapsule getHeader() {
        return recordHeader;
    }

    protected int row = 0;

    protected Connection con = null;
    protected Statement stmt = null;
    protected ResultSet rs = null;
    protected ResultSetMetaData rsmd = null;
    protected int fieldcount = 0;
    protected String query = null;
    protected boolean beforeFirst = true;
    protected RecordCapsule recordHeader = null;
    protected String rowtag = "ROW";
    protected String keypfx = "";

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DBSourceReader");

}
