/**
 *
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * The XMLBuilder class converts a RecordCapsule into a XML document. Both data
 * and meta-data are preserved in the resulting document. This is intended to be
 * a fairly lightweight operation in that a Document object is not created and
 * built using any DOM protocols, rather a simple string buffer is accumulated
 * and output.
 *
 * @author gfa
 */
public class XMLBuilder extends NullBuilder {

    /**
     * Default constructor. Will cause default behavior to not create a root tag
     * nor a standard XML document preamble.
     */
    public XMLBuilder() {
    }

    /**
     * Creates an instance of XMLBuilder that inserts a root document tag as
     * well as optionaly a standard XML preamble.
     *
     * @param root     A String to use for the root tag of the document
     * @param preamble If true, inserts a standard XML preamble.
     */
    public XMLBuilder(String root, boolean preamble) {
        rootName = root;
        doPreamble = preamble;
    }

    /**
     * This implementation of buildRecord creates a XML document (or fragment
     * depending on the value of the doPreamble and rootName
     *
     * @param rc A RecordCapsule object.
     * @return A String object containing the XML representation of a
     * RecordCapsule.
     */

    public Object buildRecord(RecordCapsule rc) {
        String retval = null;

        if (rc != null) {
            StringBuffer sb = new StringBuffer();
            if (doPreamble) {
                sb.append(writeXMLPreamble());
            }

            // Open a root level tag if specified, otherwise assume we are just
            // outputting a document fragment
            if (rootName != null) {
                sb.append(openTag(rootName));
            }

            for (int i = 0; i < rc.getFieldCount(); i++) {
                DataCapsule dc = rc.getField(i);
                sb.append(openTag(dc));
                sb.append(dc.getData().toString());
                sb.append(closeTag(dc.getName()));
            }

            // Close a root level tag if specified, otherwise assume we are
            // just
            // outputting a document fragment
            if (rootName != null) {
                sb.append(closeTag(rootName));
            }
            sb.append("\n");
            retval = sb.toString();
        }
        return retval;
    }

    protected static String closeTag(String tag) {
        String retval = "</" + tag + ">";
        return retval;
    }

    protected static String openTag(String tag) {
        String retval = "<" + tag + ">";
        return retval;
    }

    protected static String startTag(String tag) {
        return "<" + tag + " ";
    }

    protected static String endTag(String tag) {
        return ">";
    }

    protected static String openTag(DataCapsule dc) {
        String retval = null;
        StringBuffer sb = new StringBuffer();
        sb.append("<");
        sb.append(dc.getName() + " ");
        Iterator<Entry<String, Object>> it = dc.getAllMetaData();
        if (it != null) {
            while (it.hasNext()) {
                Entry<String, Object> entry = it.next();
                String name = entry.getKey();
                String value = entry.getValue().toString();
                sb.append(addAttribute(name, value));
            }
        }
        sb.append(">");
        retval = sb.toString();
        return retval;
    }

    protected static String addAttribute(String name, String value) {
        String retval = null;
        retval = name + "=\"" + value + "\" ";
        return retval;
    }

    protected static String writeXMLPreamble() {
        String retval = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>"; //$NON-NLS-1$
        return retval;
    }

    protected String rootName = null; //$NON-NLS-1$
    protected boolean doPreamble = false;
}
