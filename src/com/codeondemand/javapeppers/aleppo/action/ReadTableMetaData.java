/**
 *
 */
package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;
import com.codeondemand.javapeppers.habanero.util.db.DbUtil;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.util.TreeMap;

/**
 * This class is an implementation of SourceReader in the context of a database
 * read.
 *
 * @author gfa
 */
public class ReadTableMetaData extends RecordProcessor {

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
    public RecordCapsule processRecord(RecordCapsule input) {
        RecordCapsule retval = null;
        if (!initialized) {
            initialized = doInitialization(input);
        }

        System.out.println(input.toString());
        if ((input.checkField("SCHEMA") || input.checkField("TABLE_SCHEMA")) && input.checkField("TABLE_NAME") && input.checkField("FIELDS") && con != null) {
            String schema = null;
            if (input.checkField("TABLE_SCHEMA")) {
                schema = input.getField("TABLE_SCHEMA").getData().toString();
                input.getField("TABLE_SCHEMA").setName("SCHEMA");
            }
            schema = input.getField("SCHEMA").getData().toString();

            String table = input.getField("TABLE_NAME").getData().toString();
            String fields = input.getField("FIELDS").getData().toString();
            input.addDataCapsule(new DataCapsule("schema", schema), false);
            input.addDataCapsule(new DataCapsule("table_name", table), false);
            input.addDataCapsule(new DataCapsule("keypfx", keypfx), false);
            String squery = "alter session set current_schema =" + schema;
            query = "Select " + fields + " from " + table;
            query2 = "Select " + fields + " from " + schema + "." + table;
            //System.out.println(query);

            try {
                stmt = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                // System.out.println( stmt);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            try {
                if (stmt != null) {
                    boolean useq2 = false;
                    Statement qstmt = con.createStatement();
                    if (qstmt != null) {
                        try {
                            qstmt.execute(squery);
                            qstmt.close();
                        } catch (Exception e) {
                            useq2 = true;
                            qstmt.close();
                        }
                    }

                    stmt.setMaxRows(1);
                    if (useq2) {
                        stmt.execute(query2);
                    } else {
                        stmt.execute(query);
                    }
                    rs = stmt.getResultSet();

                    if (rs != null) {
                        rsmd = rs.getMetaData();
                        setTypeTable(DbUtil.buildTypeTable(rsmd));
                        setTypeSizeTable(DbUtil.buildTypeSizeTable(rsmd));
                        setTypeNameTable(DbUtil.buildTypeNameTable(rsmd));
                        setTypePrecisionTable(DbUtil.buildTypeScaleTable(rsmd));
                        fieldcount = rsmd.getColumnCount();
                        RecordCapsule foo = new RecordCapsule("Field_data", null);
                        for (int i = 1; i <= fieldcount; i++) {
                            String name = rsmd.getColumnName(i);
                            // String mappedName = mapFieldName(name); // not
                            // being used right now
                            int type = rsmd.getColumnType(i);
                            addField(foo, name, type, null);
                            System.out.println(name + ":" + type);
                            // foo.addDataCapsule(new
                            // DataCapsule(rsmd.getColumnName(i), null), false);
                        }

                        input.addDataCapsule(foo, false);

                        retval = input;

                        rs.close();

                        System.out.println(retval);

                        stmt.close();
                    }
                }
            } catch (SQLException e) {
                logger.error(e.toString());
                e.printStackTrace();
            }

        } else {
            logger.error("Insufficient parameters in DataCapsule."); //$NON-NLS-1$
        }

        return retval;
    }

    // private static String mapFieldName( String name){
    // String foo = name;
    // foo = foo.replace("$", "_dlr_");
    // foo = foo.replace("*", "_astk_");
    // foo = foo.replace("(", "_op_");
    // foo = foo.replace(")", "_cp_");
    // return foo;
    // }

    public boolean doInitialization(RecordCapsule input) {
        boolean retval = true;
        if (pmap.containsKey("keypfx")) {
            keypfx = (String) pmap.get("keypfx");
            if (props != null) {
                String dburl = props.getProperty(keypfx + "db.url"); //$NON-NLS-1$
                String dbuser = props.getProperty(keypfx + "db.uid"); //$NON-NLS-1$
                String dbpwd = decryptPWD(props.getProperty(keypfx + "db.pwd")); //$NON-NLS-1$
                String driver = props.getProperty(keypfx + "db.driver"); //$NON-NLS-1$

                // System.out.println(dburl+"\n"+driver+"\n"+
                // dbuser+"\n"+dbpwd);
                DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);

                dbmgr.registerDriver(driver);
                // System.out.println( dbmgr.toString());
                con = dbmgr.getConnection();
                // System.out.println( con);
                /*
				 * try { stmt =
				 * con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				 * java.sql.ResultSet.CONCUR_READ_ONLY); // System.out.println(
				 * stmt); } catch (SQLException e) { // TODO Auto-generated
				 * catch block e.printStackTrace(); }
				 */
            } else {
                logger.error("No properties available for initializing reader.");
                retval = false;
            }
        }
        return retval;
    }

    @Override
    public void done() {
        // TODO Auto-generated method stub

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
        return true;
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
        // TO-DO: what should go here?
    }

    protected void addField(RecordCapsule rc, String name, int type, Object value) {

        // System.out.println(name+":"+typeNameTable.get(name)+":"+typeSizeTable.get(name)+":"+typeScaleTable.get(name));

        DataCapsule d = new DataCapsule(name, value);
        d.setMetaData("type", type); //$NON-NLS-1$
        d.setMetaData("typeName", typeNameTable.get(name));
        d.setMetaData("length", typeSizeTable.get(name));
        d.setMetaData("scale", typeScaleTable.get(name));
        rc.addDataCapsule(d, false);

    }

    protected String decryptPWD(String input) {
        String retval = null;
        retval = MiscUtil.decodeB64String(input);
        return retval;
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
    protected String query2 = null;
    protected boolean beforeFirst = true;
    protected RecordCapsule recordHeader = null;
    protected String rowtag = "ROW";
    protected String keypfx = "";
    private boolean initialized = false;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ReadTableMetaData");

    @Override
    public boolean doInitialization() {
        // TODO Auto-generated method stub
        return false;
    }

}
