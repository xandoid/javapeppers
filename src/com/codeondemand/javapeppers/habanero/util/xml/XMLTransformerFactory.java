/**
 * 
 */

package com.codeondemand.javapeppers.habanero.util.xml;

import java.io.File;
import java.io.ObjectInputStream;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.URIResolver;
import javax.xml.transform.stream.StreamSource;


import org.apache.logging.log4j.LogManager;

import net.sf.saxon.Configuration;
import net.sf.saxon.PreparedStylesheet;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;

/**
 * The XMLTransformerFactory converts XSLT files (either compiled
 * if a Transformer is desired, or plain text if a s9api.XsltExecutable
 * is desired.
 * 
 * @version 1.0
 * @author Gary Anderson
 * 
 */
public class XMLTransformerFactory {

	private XMLTransformerFactory() {

	}

	/**
	 * This method creates an instance of a com.sf.saxon.s9api.XsltExecutable
	 * from a file containing an XSLT stylesheet.
	 * 
	 * @param p A Processor object 
	 * 
	 * @param xslfile The name of the file that contains the XSL stylesheet.
	 * 
	 * @return an XsltExecutable
	 */
	public static XsltExecutable createS9APIExecutable(Processor p,
			String xslfile) {
		XsltExecutable ret = null;
		XsltCompiler xc = p.newXsltCompiler();

		xc.setURIResolver(new URIResolver() {

			public Source resolve(String href, String base)
					throws TransformerException {

				String filename = new File(".", href).toString();
				Source source = new StreamSource(ClassLoader
						.getSystemResourceAsStream(filename));
				return source;
			}
		});
		try {

			String filename = new File(".", xslfile).toString();

			Source source = new StreamSource(
					ClassLoader.getSystemResourceAsStream(filename));
			logger.debug("Compiling transform:" + filename);
			ret = xc.compile(source);
		} catch (Exception e) {
			logger.error(e.toString());
		}
		return ret;
	}

	/**
	 * Creates a Transformer from a compiled XSLT stylesheet.
	 * 
	 * @param xslfile A compiled stylesheet.
	 * 
	 * @return A Transformer.
	 */
	public static Transformer createTransformer(String xslfile) {
		Transformer t = null;
		try {
			Configuration configuration = new Configuration();
			configuration.setURIResolver(new URIResolver() {

				public Source resolve(String href, String base)
						throws TransformerException {

					String filename = new File(".", href).toString();
					Source source = new StreamSource(ClassLoader
							.getSystemResourceAsStream(filename));
					return source;
				}
			});
			
			PreparedStylesheet p = PreparedStylesheet.loadCompiledStylesheet(
					configuration,
					new ObjectInputStream(ClassLoader
							.getSystemResourceAsStream(new File(".", xslfile)
									.toString())));
			t = p.newTransformer();
		} catch (Exception e) {
			logger.error(e.toString());
			e.printStackTrace();
		}

		return t;
	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("XMLTransformerFactory");
}
