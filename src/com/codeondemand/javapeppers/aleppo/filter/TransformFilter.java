package com.codeondemand.javapeppers.aleppo.filter;

import java.io.StringReader;

import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.xml.XMLTransformerFactory;

public class TransformFilter extends RecordFilter {

	@Override
	protected RecordCapsule filterRecord(RecordCapsule input) {
		return input;
	}

	public boolean doInitialization(){
		boolean retval = false;
		Processor p = new Processor(false);
		if (pmap.containsKey("file") && pmap.get("file") instanceof String) {
			String file = (String)pmap.get("file");
			XsltExecutable x = XMLTransformerFactory.createS9APIExecutable(p, file);
			xformer = x.load();
			
			// We are building something from nothing, so start with a context document that
			// is basically empty
			String doc = AleppoMessages.getString("TransformFilter.0"); //$NON NLS-1$ //$NON-NLS-1$
			DocumentBuilder db = p.newDocumentBuilder();
			
			retval = true;
			
			try {
				docnode = db.build(new StreamSource(new StringReader(doc)));
			} catch (SaxonApiException e) {
				logger.error(e.toString());
			}			
		}
		
		return retval;
	}

	@SuppressWarnings("unused")
	private XsltTransformer xformer = null;
	
	@SuppressWarnings("unused")
	private XdmNode docnode = null;
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("TransformFilter");

	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}
}
