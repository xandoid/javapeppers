/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
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
public class DDLBuilder extends NullBuilder {

    private String table = AleppoConstants.ALEPPO_DB_TABLE_KEY;
    private String schema = AleppoConstants.ALEPPO_DB_SCHEMA_KEY;

    public DDLBuilder() {
    }

    /**
     * Instantiates a DDLBuilder providing the names of the SCHEMA and TABLE to
     * be output in the DDL file.
     *
     * @param schema The name of the target schema.
     * @param table  The name of the target table.
     */
    public DDLBuilder(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    /**
     * This method expects the parameter map to have values for the constants
     * with names specified by
     */
    public boolean doInitialization() {
        boolean retval = true;
        if (pmap.containsKey(AleppoConstants.ALEPPO_DB_SCHEMA_KEY) && pmap.containsKey(AleppoConstants.ALEPPO_DB_TABLE_KEY)) {
            table = (String) pmap.get(AleppoConstants.ALEPPO_DB_TABLE_KEY);
            schema = (String) pmap.get(AleppoConstants.ALEPPO_DB_SCHEMA_KEY);
        } else {
            logger.debug("No 'schema' and/or 'table' attributes provided in the)" + " configuration file.");
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
        String retval = null;
        if (rc != null && rc.getFieldCount() > 0) {
            StringBuffer sb = new StringBuffer();
            sb.append(buildStart());
            colnum = 0;
            for (int i = 0; i < rc.getFieldCount(); i++) {
                DataCapsule dc = rc.getField(i);
                if (dc != null) {

                    boolean isNullable = !rc.isKey(dc.getName()); //$NON-NLS-1$
                    if (dc.getMetaData("typeName") != null && dc.getMetaData("length") != null) {
                        sb.append(addColumn(dc.getName(), dc.getMetaData("typeName").toString(), //$NON-NLS-1$
                                Integer.parseInt((dc.getMetaData("length").toString())), isNullable)); //$NON-NLS-1$

                    }
                }
                colnum++;
            }
            sb.append(buildEnd());
            if (sb.length() > 0) {
                retval = sb.toString();
            }
        }
        // Output the result to the log.
        logger.debug("DDL built: \n" + retval);

        return retval;
    }


    private String addColumn(String name, String type, int len, boolean nullable) {
        StringBuffer sb = new StringBuffer();
        if (colnum > 0) {
            sb.append(","); //$NON-NLS-1$
        }
        sb.append("\n\t" + name + "\t"); //$NON-NLS-1$ //$NON-NLS-2$
        if (type.equalsIgnoreCase("CHAR")) { //$NON-NLS-1$
            sb.append("CHAR(" + len + ")\t"); //$NON-NLS-1$ //$NON-NLS-2$
        } else if (type.equalsIgnoreCase("VARCHAR")) { //$NON-NLS-1$
            sb.append("VARCHAR(" + len + ")\t"); //$NON-NLS-1$ //$NON-NLS-2$
        } else if (type.equalsIgnoreCase("DECIMAL")) { //$NON-NLS-1$
            sb.append("DECIMAL(10,2)\t"); //$NON-NLS-1$
        } else {
            sb.append(type + "\t"); //$NON-NLS-1$
        }
        if (!nullable) {
            sb.append(" NOT NULL"); //$NON-NLS-1$
        }
        return sb.toString();
    }

    private static String buildEnd() {
        return "\n);\n";
    }

    private String buildStart() {

        return ("--DROP TABLE " + schema + "." + table + ";\n") + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                "CREATE TABLE " + schema + "." + table + "(" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                ;

    }

    // Class specific log4j logger
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DDLBuilder");

    private int colnum = 0;
}
