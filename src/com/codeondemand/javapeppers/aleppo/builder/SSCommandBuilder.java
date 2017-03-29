/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

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
public class SSCommandBuilder extends NullBuilder {


    public SSCommandBuilder() {
    }

    /**
     * This method expects the parameter map to have values for the constants
     * with names specified by
     */
    public boolean doInitialization(RecordCapsule rc) {
        boolean retval = true;
        if (props.containsKey("ss.env")) {
            ss_env = props.getProperty("ss.env");
        }
        if (props.containsKey("ss.server") && props.containsKey("ss.port")) {
            ss_url = "https://" + props.getProperty("ss.server") + ":" + props.getProperty("ss.port");
        } else {
            logger.error("No value for ss_url parameter found.");
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
        if (rc.checkField("SCHEMA") && rc.checkField("TABLE_NAME") && rc.checkField("SOURCE")) {
            schema = rc.getField("SCHEMA").getData().toString().toLowerCase();
            table = rc.getField("TABLE_NAME").getData().toString().toLowerCase();
        }

        if (rc.checkField("import_output_file_name")) {
            rc.getField("import_output_file_name").setData(new String("import_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
        } else {
            DataCapsule dc = new DataCapsule("import_output_file_name", new String("import_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
            rc.addDataCapsule(dc, false);
        }
        if (rc.checkField("delete_output_file_name")) {
            rc.getField("delete_output_file_name").setData(new String("delete_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
        } else {
            DataCapsule dc = new DataCapsule("delete_output_file_name", new String("delete_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
            rc.addDataCapsule(dc, false);
        }

        if (rc.checkField("reset_output_file_name")) {
            rc.getField("reset_output_file_name").setData(new String("reset_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
        } else {
            DataCapsule dc = new DataCapsule("reset_output_file_name", new String("reset_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
            rc.addDataCapsule(dc, false);
        }
        if (rc.checkField("start_output_file_name")) {
            rc.getField("start_output_file_name").setData(new String("start_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
        } else {
            DataCapsule dc = new DataCapsule("start_output_file_name", new String("start_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
            rc.addDataCapsule(dc, false);
        }
        if (rc.checkField("status_output_file_name")) {
            rc.getField("status_output_file_name").setData(new String("status_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
        } else {
            DataCapsule dc = new DataCapsule("status_output_file_name", new String("status_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
            rc.addDataCapsule(dc, false);
        }


        if (rc.checkField("stop_output_file_name")) {
            rc.getField("stop_output_file_name").setData(new String("stop_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
        } else {
            DataCapsule dc = new DataCapsule("stop_output_file_name", new String("stop_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase() + ".sh"));
            rc.addDataCapsule(dc, false);
        }

        rc.addDataCapsule(new DataCapsule("import_command", buildImportCommand()), false);
        rc.addDataCapsule(new DataCapsule("delete_command", buildDeleteCommand()), false);
        rc.addDataCapsule(new DataCapsule("reset_command", buildResetCommand()), false);
        rc.addDataCapsule(new DataCapsule("start_command", buildStartCommand()), false);
        rc.addDataCapsule(new DataCapsule("stop_command", buildStopCommand()), false);
        rc.addDataCapsule(new DataCapsule("status_command", buildStatusCommand()), false);
        return rc;
    }

    private String buildImportCommand() {

        return "#!/bin/bash\n\n" + "# need to keep the password in a file owned only by the executing user\n" + "foo=`cat ../../pwd.txt`\n\n" + "bar=`cat ../../uid.txt`\n\n" + "ssurl=`cat ../../ss_server.txt`\n\n" + "# Shell script for importing a streamsets pipeline into a local pipeline instance\n" + "# Created automatically by a javapeppers workstream.\n\n" + "streamsets cli -u $bar -p $foo -U $ssurl store import -n " + dbout + "_" + schema + "_" + table + " -f ./" + dbout + "_" + schema + "_" + table + ".json \n" + "";

    }

    private String buildDeleteCommand() {

        return "#!/bin/bash\n\n" + "# need to keep the password in a file owned only by the executing user\n" + "foo=`cat ../../pwd.txt`\n\n" + "bar=`cat ../../../uid.txt`\n\n" + "ssurl=`cat ../../../ss_server.txt`\n\n" + "# Shell script for deleting a streamsets pipeline into a local pipeline instance\n" + "# Created automatically by a javapeppers workstream.\n\n" + "streamsets cli -u $bar -p $foo -U $ssurl store delete -n " + dbout + "_" + schema + "_" + table + "\n" + "";

    }

    private String buildResetCommand() {

        return "#!/bin/bash\n\n" + "# need to keep the password in a file owned only by the executing user\n" + "foo=`cat ../../../pwd.txt`\n\n" + "bar=`cat ../../../uid.txt`\n\n" + "ssurl=`cat ../../../ss_server.txt`\n\n" + "# Shell script for resetting the origin of a streamsets pipeline into a local pipeline instance\n" + "# Created automatically by a javapeppers workstream.\n\n" + "streamsets cli -u $bar -p $foo -U $ssurl  manager reset-origin -n " + dbout + "_" + schema + "_" + table + "\n" + "";

    }

    private String buildStartCommand() {

        return "#!/bin/bash\n\n" + "# need to keep the password in a file owned only by the executing user\n" + "foo=`cat ../../../pwd.txt`\n\n" + "bar=`cat ../../../uid.txt`\n\n" + "ssurl=`cat ../../../ss_server.txt`\n\n" + "# Shell script for starting a streamsets pipeline on a local pipeline instance\n" + "# Created automatically by a javapeppers workstream.\n\n" + "streamsets cli -u $bar -p $foo -U $ssurl  manager start -n " + dbout + "_" + schema + "_" + table + "\n" + "";

    }

    private String buildStopCommand() {

        return "#!/bin/bash\n\n" + "# need to keep the password in a file owned only by the executing user\n" + "foo=`cat ../../../pwd.txt`\n\n" + "bar=`cat ../../../uid.txt`\n\n" + "ssurl=`cat ../../../ss_server.txt`\n\n" + "# Shell script for stopping a streamsets pipeline on a local pipeline instance\n" + "# Created automatically by a javapeppers workstream.\n\n" + "streamsets cli -u $bar -p $foo -U $ssurl  manager stop -n " + dbout + "_" + schema + "_" + table + "\n" + "";

    }

    private String buildStatusCommand() {

        return "#!/bin/bash\n\n" + "# need to keep the password in a file owned only by the executing user\n" + "foo=`cat ../../../pwd.txt`\n\n" + "bar=`cat ../../../uid.txt`\n\n" + "ssurl=`cat ../../../ss_server.txt`\n\n" + "# Shell script for getting status of a streamsets pipeline on a local pipeline instance\n" + "# Created automatically by a javapeppers workstream.\n\n" + "streamsets cli -u $bar -p $foo -U $ssurl  manager status -n " + dbout + "_" + schema + "_" + table + "\n" + "";

    }


    // Class specific log4j logger
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SSCommandBuilder");


    private boolean initialized = false;
    private String table = "";
    private String schema = "";
    private String ss_url = "";
    private String ss_env = "";
    private String dbout = "";

}
