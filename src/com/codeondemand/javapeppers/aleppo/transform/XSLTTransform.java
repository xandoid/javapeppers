/**
 *
 */
package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.xml.XMLTransformerFactory;
import net.sf.saxon.s9api.*;
import org.apache.logging.log4j.LogManager;

import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * The XSLTTransform class is designed to apply an XSL Transform to the
 * document contained in one or more RecordCapsule fields (DataCapsules)
 * that are specified in the configuration file using the xformnames parameter.
 * Typically there will be only one block of data to transform, but it does
 * not need to be limited to one.  The XSLT that is applied needs to be
 * specified in the configuration file using the "file" parameter.  An example
 * of this class using an XSLT transform file (must be in classpath) and
 * requesting two named fields to be transformed would look like this:
 * <p>
 * <pre>
 *  &lt;dataflow&gt;
 *  ... (source of data flow)
 *  &lt;process&gt;
 *      &lt;transform class="com.codeondemand.javapeppers.aleppo.transform.XSLTTransform"
 *                    file="somefile.xsl" xformnames="field1:file3" /&gt;
 *  &lt;process/&gt;
 *   ... (destination of data flow)
 *   &lt;dataflow/&gt;
 *  </pre>
 *
 * @author Gary Anderson
 */

public class XSLTTransform extends RecordTransform {

    public boolean doInitialization() {
        boolean retval = false;
        p = new Processor(false);
        db = p.newDocumentBuilder();
        if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE) && pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE) instanceof String) {
            String filename = (String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_FILE);
            XsltExecutable x = null;
            x = XMLTransformerFactory.createS9APIExecutable(p, filename);
            xformer = x.load();

            logger.debug("Creating executable from stylesheet: " + filename);

            x = XMLTransformerFactory.createS9APIExecutable(p, filename);
            if (x != null) {
                xformer = x.load();
            } else {
                logger.error("Unable to create XSLT transformer.");
            }

            if (pmap.containsKey(AleppoConstants.ALEPPO_TRANSFORM_XFORMNAMES_KEY)) {
                String[] foo = ((String) pmap.get(AleppoConstants.ALEPPO_TRANSFORM_XFORMNAMES_KEY)).split(":");
                for (String aFoo : foo) {
                    names.add(aFoo);
                    logger.debug("XFORMNAME: " + aFoo);
                }
            }
            if (pmap.containsKey(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_TOKENIZE)) {
                tokenize = Boolean.valueOf((String) pmap.get(AleppoConstants.ALEPPO_CONFIG_ALL_PARAM_TOKENIZE));
                logger.debug("Setting tokenize to " + tokenize);
            }
        }

        retval = xformer != null ? true : false;

        return retval;
    }

    public RecordCapsule doTransform(RecordCapsule input) {
        RecordCapsule retval = input;
        for (int i = 0; i < input.getFieldCount(); i++) {
            if (!input.getField(i).isNull() && (names.contains(input.getField(i).getName()) || names.size() == 0)) {
                XdmDestination d = new XdmDestination();
                xformer.setDestination(d);

                docnode = buildDoc(input.getField(i));
                if (docnode != null) {

                    xformer.setInitialContextNode(docnode);
                    try {
                        setParameters(input);
                        xformer.transform();
                        if (tokenize) {
                            String temp = d.getXdmNode().toString();
                            StringTokenizer st = new StringTokenizer(temp, "|");
                            while (st.hasMoreTokens()) {
                                String n = st.nextToken().trim();
                                if (st.hasMoreTokens()) {
                                    String v = st.nextToken().trim();
                                    input.addDataCapsule(new DataCapsule(n, v), false);
                                }
                            }
                        } else {
                            if (names.size() > 0) {
                                input.getField(i).setData(d.getXdmNode().toString());
                            } else {
                                input.addDataCapsule(new DataCapsule("xform", d.getXdmNode().toString()), false);
                                break;
                            }
                        }
                    } catch (SaxonApiException e) {
                        logger.error(e.toString());
                        retval = null;
                    }
                }
            }
        }

        return retval;
    }

    protected XdmNode buildDoc(DataCapsule input) {
        XdmNode retval = null;

        if (!input.isNull() && input.getData() instanceof String) {
            try {
                retval = db.build(new StreamSource(new StringReader(input.getData().toString())));
            } catch (SaxonApiException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return retval;
    }

    protected boolean setParameters(RecordCapsule input) {
        return true;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("XSLTTransform");

    protected DocumentBuilder db = null;
    protected XsltTransformer xformer = null;
    private XdmNode docnode = null;
    private ArrayList<String> names = new ArrayList<>(1);
    private boolean tokenize = false;
    net.sf.saxon.s9api.Processor p = null;


    public void done() {
        // TODO Auto-generated method stub

    }
}
