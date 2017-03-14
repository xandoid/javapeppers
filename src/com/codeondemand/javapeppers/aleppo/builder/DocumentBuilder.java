/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.builder;

import com.itextpdf.text.Document;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public class DocumentBuilder extends NullBuilder {

	public Object buildHeader(RecordCapsule r) {
		return null;
	}

	public Object buildRecord(RecordCapsule r) {
		Document doc = new Document();
		doc.addAuthor("gfa");
		doc.addSubject("test subject");
		return doc;
	}
//	private static org.apache.commons.logging.Log logger = LogFactory
//	.getLog(DocumentBuilder.class);

	@Override
	public boolean doInitialization() {
		// TODO Auto-generated method stub
		return false;
	}
}
