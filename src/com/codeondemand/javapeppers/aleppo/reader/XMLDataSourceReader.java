package com.codeondemand.javapeppers.aleppo.reader;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

import java.sql.Connection;
import java.util.HashSet;
import java.util.StringTokenizer;

// gfa - Removed creation of a XdmNode for adding to the DataCapsule
//       since the Object could not be read in another context as it
//       was passed through the flow.

public class XMLDataSourceReader extends DBSourceReader {

    //	private XdmNode docnode = null;
//	private Processor p = null;
    private String dataname = "data";
    private StringBuffer buffer = null;
    private HashSet<String> keys = null;


    @Override
    public boolean initialize(Connection dbcon, String dbquery) {

        boolean value = super.initialize(dbcon, dbquery);

        if (pmap.containsKey("key_fields")) {
            keys = new HashSet<>();
            StringTokenizer st = new StringTokenizer((String) pmap.get("key_fields"), "|");
            while (st.hasMoreTokens()) {
                keys.add(st.nextToken());
            }
        }
        if (pmap.containsKey("data_name")) {
            dataname = (String) pmap.get("data_name");
        }
//		p = new Processor(false);
        return value;
    }

    // Called when a new record is read from the database.
    //
    //	1) Delegates creation of a RecordCapsule to the superclass.
    //  2) Starts a new StringBuffer and seeds it with the opening tag
    //
    protected RecordCapsule newRecord(String name) {
        RecordCapsule retval = super.newRecord(name);
        buffer = new StringBuffer();
        buffer.append("<" + rowtag + " rownum=\"");
        buffer.append(row + "\">");
        return retval;
    }

    // Called for each column of the record.  It just adds a tag named for the
    // column and adds parameters for type and value.
    protected void addField(RecordCapsule rc, String name, int type, Object value) {
        if (keys != null && keys.contains(name)) {
            rc.addDataCapsule(new DataCapsule(name, value), true);
        }
        buffer.append("<" + name + " type =\"" + type + "\" value=\"" + value + "\"/>");
    }

    // This class completes the XML fragment with a closing tag and then
    // parses it to an XdmNode which it adds as the payload of a DataCapsule
    protected void endRecord(RecordCapsule rc) {
        buffer.append("</" + rowtag + ">");
        DataCapsule dc = new DataCapsule(dataname, buffer.toString());
        rc.addDataCapsule(dc, false);
//		try {
//			DocumentBuilder db = p.newDocumentBuilder();
//			docnode = db.build(new StreamSource(new StringReader(buffer
//					.toString())));
//          DataCapsule dc = new DataCapsule(dataname, docnode);
//		} catch (SaxonApiException e) {
//			logger.error(e.getLocalizedMessage());
//		}


    }

}
