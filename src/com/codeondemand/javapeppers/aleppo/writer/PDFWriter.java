/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.writer;

import org.apache.logging.log4j.LogManager;

import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Font;
import com.itextpdf.text.List;
import com.itextpdf.text.ListItem;
import com.itextpdf.text.Paragraph;
//import com.itextpdf.text.pdf.PdfWriter;

public class PDFWriter extends DestinationWriter {

	public boolean close() {
		return false;
	}

	public boolean reset() {
		return false;
	}

	public boolean write(Object data) {
		boolean retval = false;
		if (data instanceof Document) {
			try {
				Document doc = (Document) data;
				//PdfWriter wr = PdfWriter.getInstance((Document) data, new FileOutputStream(
				//		"mark_ramsey.pdf"));
				doc.open();
				Paragraph p = new Paragraph("Partner Performance Report:" + "\t\tRamsey, M\n\n", new Font(Font
						.getFamily("HELVETICA"), 18, Font.BOLDITALIC, new BaseColor(0, 0, 255)));
				doc.add(p);
				List list = new List(true, 20);
				list.setFirst(1);
				list.add(new ListItem("Contents of report"));
				List list2 = new List(true,20);
				list2.setLettered(true);
				list2.add(new ListItem("Revenue action items"));
				list2.add(new ListItem("Signings"));
				list2.add(new ListItem("Utilization summary"));
				list.add(list2);
				doc.add(list);
				doc.add(new Chunk("\n\n\n\tTest document\n\t\t(created by com.javapeppers.aleppo.writer.PDFWriter)"));
				doc.close();
			} catch (DocumentException e) {
				logger.error(e.toString());
			}
			retval = true;
		} else {
			logger.error("Invalid object passed to PDFWriter");
		}
		return retval;
	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("PDFWriter");

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
}
