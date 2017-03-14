/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.reader;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


import org.apache.logging.log4j.LogManager;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XMLDocReader extends FileSourceReader {

	public XMLDocReader() {
	}

	public boolean initialize(String filename, String nodename) {
		boolean retval = false;
		this.nodename = nodename;
		item = 0;
		retval = super.initialize(filename);
		try {
			dbldr = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		StringBuffer querysb = new StringBuffer();
		while( true ){
			String foo = (String)super.read();
			if( foo == null ){
				break;
			}else{
				querysb.append(foo.trim()+" ");
			}
		}
		String temp = querysb.toString();
		if (temp != null) {
			Document doc = null;
			try {
				doc = dbldr.parse(new InputSource(new java.io.StringReader(temp)));
				nlist = doc.getElementsByTagName(nodename);
			} catch (SAXException e) {
				logger.error("Error parsing input document:" + ":"+ filename + ":"+ e.toString());
			} catch (IOException e) {
				logger.error("Error reading input document:"+ filename+ ":" + e.toString());
			}
		}
		return retval;
	}
	
	public Object read(){
		Object retval = null;
		if( item < nlist.getLength() ){
			retval = nlist.item(item);
			item++;
		}
		return retval;
	}

	private int item = 0;
	@SuppressWarnings("unused")
	private String nodename = null;
	private DocumentBuilder dbldr = null;
	private NodeList nlist = null;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("XMLDocReader");

}
