/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

/**
 * The DDLBuilder class converts the information contained in the meta-data of
 * the DataCapsules into a specification that can be used to create a table for
 * loading the output data stream. The DDL specification will need to be edited
 * or converted in some other fashion to provide the desired name for the schema
 * and table to be used. This can be also done at the time of instantiating this
 * builder.
 *
 * @author gfa
 */
public class HiveTableDefinitionBuilder extends NullBuilder {

    private String table = AleppoConstants.ALEPPO_DB_TABLE_KEY;
    private String schema = AleppoConstants.ALEPPO_DB_SCHEMA_KEY;
    private String delim = "";
    private String format = "";
    private String partition = null;
    private String dbname = "";
    private String dbout = "";

    public HiveTableDefinitionBuilder() {
    }

    /**
     * Instantiates a DDLBuilder providing the names of the SCHEMA and TABLE to
     * be output in the DDL file.
     *
     * @param schema The name of the target schema.
     * @param table  The name of the target table.
     */
    public HiveTableDefinitionBuilder(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    /**
     * This method expects the parameter map to have values for the constants
     * with names specified by
     */
    public boolean doInitialization(RecordCapsule rc) {
        boolean retval = true;
        if (pmap.containsKey("dbname")) {
            dbname = (String) pmap.get("dbname");
            dbname = MiscUtil.mapString(props, dbname, "%");
        }

        if (pmap.containsKey("dbout")) {
            dbout = (String) pmap.get("dbout");
            dbout = MiscUtil.mapString(props, dbout, "%");
        }
        return retval;
    }

    /**
     * This builds the entire DDL document using the data in the RecordCapsule
     * and its DataCapsule children.
     *
     * @param rc The RecordCapsule containing the data structure information.
     * @return Returns a the DDL document as a single String object.
     */
    public Object buildRecord(RecordCapsule rc) {
        if (!initialized) {
            initialized = doInitialization(rc);
        }
        if (rc.checkField("SCHEMA") && rc.checkField("TABLE_NAME")) {
            schema = rc.getField("SCHEMA").getData().toString();
            table = rc.getField("TABLE_NAME").getData().toString();
        }
        if (pmap.containsKey("delim")) {
            delim = (String) pmap.get("delim");
            if (delim.equals("PIPE")) {
                delim = "|";
            }
            if (delim.equals("COMMA")) {
                delim = ",";
            }
            if (delim.equals("TAB")) {
                delim = "\\t";
            }
        }

        if (rc.checkField("ddl_output_file_name")) {
            rc.getField("ddl_output_file_name").setData(new String(rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + "_definition.hql"));
        } else {
            DataCapsule dc = new DataCapsule("ddl_output_file_name", new String(rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".hql"));
            rc.addDataCapsule(dc, false);
        }

        if (pmap.containsKey("format")) {
            format = (String) pmap.get("format");
        }
        if (pmap.containsKey("partition")) {
            partition = (String) pmap.get("partition");
        }

        String retval = null;
        RecordCapsule fc = (RecordCapsule) rc.getField("Field_data");
        if (fc != null && fc.getFieldCount() > 0) {
            StringBuffer sb = new StringBuffer();
            sb.append(buildStart());
            colnum = 0;
            for (int i = 0; i < fc.getFieldCount(); i++) {
                DataCapsule dc = fc.getField(i);
                if (dc != null) {

                    boolean isNullable = !fc.isKey(dc.getName()); // $NON-NLS-1$
                    if (dc.getMetaData("typeName") != null && dc.getMetaData("length") != null) {
                        sb.append(addColumn(dc.getName(), xlate(dc.getMetaData("typeName").toString(), dc.getMetaData("length").toString(), dc.getMetaData("scale").toString()), //$NON-NLS-1$
                                Integer.parseInt((dc.getMetaData("length").toString())), Integer.parseInt((dc.getMetaData("scale").toString())), isNullable)); //$NON-NLS-1$
                    }
                }
                colnum++;
            }
            sb.append(buildEnd());
            if (sb.length() > 0) {
                retval = sb.toString().toLowerCase();
            }
        }
        // Output the result to the log.
        logger.debug("DDL built: \n" + retval);

        rc.addDataCapsule(new DataCapsule("table_def", retval), false);
        return rc;
    }

    private String addColumn(String name, String type, int len, int scale, boolean nullable) {
        StringBuffer sb = new StringBuffer();
        if (colnum > 0) {
            sb.append(","); //$NON-NLS-1$
        }
        String newname = mapFieldName(name);
        sb.append("\n     `" + newname + "` "); //$NON-NLS-1$ //$NON-NLS-2$
        if (type.equalsIgnoreCase("CHAR")) { //$NON-NLS-1$
            sb.append("CHAR(" + len + ") "); //$NON-NLS-1$ //$NON-NLS-2$
        } else if (type.equalsIgnoreCase("VARCHAR")) { //$NON-NLS-1$
            sb.append("VARCHAR(" + len + ") "); //$NON-NLS-1$ //$NON-NLS-2$
        } else if (type.equalsIgnoreCase("DECIMAL")) { //$NON-NLS-1$
            sb.append("DECIMAL(" + len + "," + scale + ") "); //$NON-NLS-1$
        } else {
            sb.append(type + " "); //$NON-NLS-1$
        }
        if (!nullable) {
            sb.append(" NOT NULL"); //$NON-NLS-1$
        }
        return sb.toString();
    }

    private String buildEnd() {
        String retval = ")\n";
        if (partition != null) {
            retval = retval + "PARTITIONED BY (" + partition + ")\n";
        }
        retval = retval + " ROW FORMAT " + format + " FIELDS TERMINATED BY '" + delim + "\' STORED AS TEXTFILE;\n"; //$NON-NLS-1$
        // retval = retval + " LOCATION
        // \'/data/warehouse/ga940865.db/"+source+"_"+schema + "_" +
        // table+"\';\n";
        return retval;
    }

    private static String xlate(String temp, String field_len, String field_scale) {
        String retval = temp;
        if (temp.equalsIgnoreCase("VARCHAR2")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("NVARCHAR2")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("DATE")) {
            retval = "TIMESTAMP";
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("NCHAR")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("CLOB")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("NCLOB")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("ROWID")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("ROWID")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("sys.xmltype")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("xmltype")) {
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("LONG")) {
            retval = "BIGINT";
        }
        if (temp.equalsIgnoreCase("LONG RAW")) {
            retval = "STRING";
        }

        if (temp.equalsIgnoreCase("DATETIME")) {
            retval = "TIMESTAMP";
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("RAW")) {
            retval = "BINARY";
        }
        if (temp.equalsIgnoreCase("BLOB")) {
            retval = "STRING";
        }
        if (temp.toUpperCase().startsWith("TIMESTAMP")) {
            retval = "TIMESTAMP";
            retval = "STRING";
        }
        if (temp.equalsIgnoreCase("NUMBER")) {
            int scale = Integer.parseInt(field_scale);
            int length = Integer.parseInt(field_len);
            if (scale < 1) {
                if (length > 10 && length < 19) {
                    retval = "BIGINT";
                } else {
                    retval = "INT";
                }
            } else {
                retval = "DECIMAL";
            }
        }
        return retval;
    }

    private String mapFieldName(String name) {
        String foo = name;
        foo = foo.replace("$", "_dlr_");
        foo = foo.replace("*", "_astk_");
        foo = foo.replace("(", "_op_");
        foo = foo.replace(")", "_cp_");
        return foo;
    }

    private String mapTableName(String name) {
        String foo = name;
        foo = foo.replace("$", "_dlr_");
        foo = foo.replace("*", "_astk_");
        foo = foo.replace("(", "_op_");
        foo = foo.replace(")", "_cp_");
        return foo;
    }

    private String buildStart() {

        String tblname = "t_" + dbout + "_" + schema + "_e_" + table;
        String sb = ("use " + dbout + ";\n") + "--DROP TABLE IF EXISTS " + mapTableName(tblname) + ";\n" + "CREATE EXTERNAL TABLE IF NOT EXISTS " + mapTableName(tblname) + "( \n\tingest_id string, \n\trid_orig string, \n\tingest_ts string,";
        // //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$



        return sb;

    }

    // Class specific log4j logger
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("HiveTableDefinitionBuilder");

    private int colnum = 0;
    private boolean initialized = false;
}
