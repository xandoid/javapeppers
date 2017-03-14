/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.parser;


import org.apache.logging.log4j.LogManager;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.FlowNode;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public class XMLNodeParser extends FlowNode implements RecordParser{

	public RecordCapsule parseRecord(Object input) {
		RecordCapsule retval = null;
		if( input instanceof Node ){
			Node n = (Node)input;
			retval = new RecordCapsule( n.getAttributes().getNamedItem("name").getNodeValue(),null);
			NodeList nl = n.getChildNodes();
			for( int i = 0; i < nl.getLength(); i++){
				Node child = nl.item(i);
				if( !child.getNodeName().trim().equals("#text")){
					retval.addDataCapsule(new DataCapsule(child.getNodeName(),child), false);
				}
			}
		}else{
			logger.error("Unexpected input object:"+input);
		}
		return retval;
	}
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("XMLNodeParser");
	
	@Override
	public boolean doInitialization() {
		return true;
	}	
}
