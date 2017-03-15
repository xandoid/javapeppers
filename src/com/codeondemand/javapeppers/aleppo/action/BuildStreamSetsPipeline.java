package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

import java.io.*;

public class BuildStreamSetsPipeline extends RecordProcessor {

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {
        if (!initialized) {
            initialized = doInitialization();
        }
        RecordCapsule retval = input;
        if (input.checkField("SOURCE")) {
            keypfx = input.getField("SOURCE").getData().toString().toLowerCase();
        }

        if (input.checkField("ss_output_file_name")) {
            input.getField("ss_output_file_name").setData(new String(dbout + "_" + input.getField("schema").getData().toString().toLowerCase() + "_" + input.getField("table").getData().toString().toLowerCase()));
        } else {
            DataCapsule dc = new DataCapsule("ss_output_file_name", new String(dbout + "_" + input.getField("SCHEMA").getData().toString().toLowerCase() + "_" + input.getField("TABLE_NAME").getData().toString().toLowerCase() + ".json"));
            input.addDataCapsule(dc, false);
        }

        if (pmap.containsKey("file")) {
            String filename = (String) pmap.get("file");
            File template = new File(filename);
            BufferedReader rdr = null;
            StringBuffer temp = new StringBuffer();
            if (template.exists()) {
                try {
                    rdr = new BufferedReader(new FileReader(template));
                    while (rdr.ready()) {
                        String rl = rdr.readLine();
                        if (rl != null) {
                            temp.append(subTokens(rl, input) + "\n");
                        }
                    }
                    rdr.close();
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            input.addDataCapsule(new DataCapsule("pipeline", temp.toString()), false);
        }
        return retval;
    }

    @Override
    public void done() {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean doInitialization() {
        boolean retval = true;
        if (pmap.containsKey("keypfx")) {
            hdfspfx = (String) pmap.get("keypfx");
        } else {
            System.out.println("no keypfx found in pmap");
        }
        if (pmap.containsKey("hdfs.uri")) {
            hdfs_uri = (String) pmap.get("hdfs.uri");
        }
        if (pmap.containsKey("hdfs.uid")) {
            hdfs_uid = (String) pmap.get("hdfs.uid");
        }

        if (pmap.containsKey("dbout")) {
            dbout = (String) pmap.get("dbout");
            dbout = MiscUtil.mapString(props, dbout, "%");
        }

        return retval;
    }

    private String subTokens(String foo, RecordCapsule rc) {
        String retval = foo;
        if (retval.contains("@UUID@")) {
            String uuid = java.util.UUID.randomUUID().toString();
            retval = retval.replace("@UUID@", uuid);
        }
        if (retval.contains("@INGESTID@")) {
            if (ingestid == null) {
                ingestid = java.util.UUID.randomUUID().toString();
            }
            retval = retval.replace("@INGESTID@", ingestid);
        }

        if (retval.contains("@UTIME@")) {
            retval = retval.replace("@UTIME@", ltime);
        }
        if (retval.contains("@HDFS_URI@")) {
            retval = retval.replace("@HDFS_URI@", (CharSequence) props.getProperty(hdfspfx + "uri"));
        }

        if (retval.contains("@QUERY1@")) {
            retval = retval.replace("@QUERY1@", genQuery(rc, false));
        }
        if (retval.contains("@QUERY2@")) {
            retval = retval.replace("@QUERY2@", genQuery(rc, true));
        }
        if (retval.contains("@PIPE_DESC@")) {
            retval = retval.replace("@PIPE_DESC@", "Auto generated pipeline from @SCHEMA@.@TABLE@ to HDFS/Hive " + MiscUtil.getCurrentTimeString());
        }
        if (retval.contains("@HDFS_UID@")) {
            retval = retval.replace("@HDFS_UID@", (CharSequence) props.get(hdfspfx + "uid"));
        }

        if (retval.contains("@SCHEMA@")) {
            retval = retval.replace("@SCHEMA@", rc.getField("SCHEMA").getData().toString().toLowerCase());
        }
        if (retval.contains("@SOURCE@")) {
            retval = retval.replace("@SOURCE@", rc.getField("SOURCE").getData().toString().toLowerCase());
        }
        if (retval.contains("@RECS_PER_FILE@")) {
            retval = retval.replace("@RECS_PER_FILE@", (CharSequence) props.getProperty(hdfspfx + "records_per_file"));
        }
        if (retval.contains("@TABLE@")) {
            retval = retval.replace("@TABLE@", rc.getField("TABLE_NAME").getData().toString().toLowerCase());
        }
        if (retval.contains("@HDFS_TABLE@")) {
            retval = retval.replace("@HDFS_TABLE@", "t_" + dbout + "_" + rc.getField("SCHEMA").getData().toString().toLowerCase() + "_e_" + rc.getField("TABLE_NAME").getData().toString().toLowerCase());
        }
        if (retval.contains("@HIVE_LOCATION@")) {
            retval = retval.replace("@HIVE_LOCATION@", "/data/warehouse/foundation/" + dbout + ".db");
            retval = retval.toLowerCase();
        }

        if (retval.contains("@OFFSET_COL@")) {
            if (rc.getField("OFFSET_COL").getData().toString().equals("ROWID")) {
                retval = retval.replace("@OFFSET_COL@", "RID");
            } else {
                retval = retval.replace("@OFFSET_COL@", rc.getField("OFFSET_COL").getData().toString());
            }
        }

        if (retval.contains("@EMAIL@") && (props.getProperty("email_address") != null)) {
            String e = props.getProperty("email_address");
            retval = retval.replace("@EMAIL@", e);
        }
        if (retval.contains("@DBOUT@")) {
            retval = retval.replace("@DBOUT@", dbout);
        }
        if (retval.contains("@UID@")) {
            String u = props.getProperty("metadata.db.uid");
            retval = retval.replace("@UID@", u);
        }
        if (retval.contains("@PWD@")) {
            String p = MiscUtil.decodeB64String(props.getProperty("metadata.db.pwd"));
            retval = retval.replace("@PWD@", p);
        }
        if (retval.contains("@URL@")) {
            String url = props.getProperty("metadata.db.url");
            if (url != null) {
                retval = retval.replace("@URL@", url);
            }
        }
        return retval;
    }

    private String genQuery(RecordCapsule rc, boolean cast) {
        String retval = null;
        StringBuffer sb = new StringBuffer();
        String tbl_select_token = "";
        String tok_delim = ",";
        RecordCapsule f = (RecordCapsule) rc.getField("Field_data");
        int cnt = f.getFieldCount();
        if (pmap.containsKey("incl_tbl")) {
            boolean incl_tbl = new Boolean((String) pmap.get("incl_tbl")).booleanValue();
            if (incl_tbl) {
                tbl_select_token = new String(tok_delim + "'" + rc.getField("TABLE_NAME").getData().toString() + "' as TBL_NAME");
            }
        }
        sb.append(tbl_select_token);
        for (int i = 0; i < cnt; i++) {
            sb.append(tok_delim);
            if (cast) {
                if (f.getField(i).getMetaData("typeName").equals("RAW")) {
                    sb.append(" RAWTOHEX(\\\"" + f.getField(i).getName() + "\\\") as \\\"" + f.getField(i).getName() + "\\\"");
                } else if (f.getField(i).getMetaData("typeName").toString().startsWith("TIMESTAMP")) {
                    sb.append(" cast(\\\"" + f.getField(i).getName() + "\\\" as VARCHAR(35)) as \\\"" + f.getField(i).getName() + "\\\"");
                } else if (f.getField(i).getMetaData("typeName").equals("BLOB")) {
                    sb.append("utl_raw.cast_to_varchar2(dbms_lob.substr(\\\"" + f.getField(i).getName() + "\\\" ,2000,0)) as \\\"" + f.getField(i).getName() + "\\\"");
                } else if (f.getField(i).getMetaData("typeName").equals("CLOB")) {
                    sb.append("utl_raw.cast_to_varchar2(dbms_lob.substr(\\\"" + f.getField(i).getName() + "\\\",2000,0)) as \\\"" + f.getField(i).getName() + "\\\"");
                } else if (f.getField(i).getName().equals("RID")) {
                    sb.append(" cast(\\\"" + f.getField(i).getName() + "\\\" as VARCHAR(35)) as \\\"" + f.getField(i).getName() + "\\\"");
                } else if (f.getField(i).getName().equals("VIEW_ROWID")) {
                    sb.append(" cast(\\\"" + f.getField(i).getName() + "\\\" as VARCHAR(35)) as \\\"" + f.getField(i).getName() + "\\\"");
                } else if (f.getField(i).getName().equals("ORIG_ROWID")) {
                    sb.append(" cast(\\\"" + f.getField(i).getName() + "\\\" as VARCHAR(35)) as \\\"" + f.getField(i).getName() + "\\\"");
                } else {
                    sb.append("\\\"" + f.getField(i).getName() + "\\\"");
                }
            } else {
                sb.append("\\\"" + f.getField(i).getName() + "\\\"");
            }
        }
        retval = sb.toString();

        return retval;
    }

    private String quotedFieldName(RecordCapsule r, int index) {
        String retval = null;
        DataCapsule dc = r.getField(index);
        if (dc != null) {
            retval = "\\\"" + r.getField(index).getName() + "\\\"";
        }
        return retval;
    }

    private String keypfx = null;
    private String dbout = null;
    private String hdfspfx = null;
    private String hdfs_uri = null;
    private String hdfs_uid = null;
    private boolean initialized = false;
    private String ingestid = null;
    private String ltime = new Long(System.currentTimeMillis()).toString();
}
