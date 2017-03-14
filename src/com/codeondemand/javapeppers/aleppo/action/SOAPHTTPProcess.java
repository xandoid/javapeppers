/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Security;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.aleppo.reader.FileSourceReader;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public class SOAPHTTPProcess extends RecordProcessor {

	public boolean doInitialization() {
		boolean retval = false;
		StringBuffer sb = new StringBuffer();
		if (pmap.containsKey("file") && pmap.get("file") instanceof String) {
			String filename = (String) pmap.get("file");
			FileSourceReader rdr = new FileSourceReader();
			rdr.initialize(filename);
			String temp = null;
			while ((temp = (String) rdr.read()) != null) {
				sb.append(temp.trim() + " ");
			}
			rdr.close();
		} else {
			logger.error("No SOAP request template file provided");
		}

		if (pmap.containsKey("url")) {
			setUrl((String) pmap.get("url"));
		}

		if (pmap.containsKey("userid")) {
			setUserid((String) pmap.get("userid"));
		}

		if (pmap.containsKey("pwd")) {
			setPwd((String) pmap.get("pwd"));
		}

		setSoapRequest(sb.toString());

		String foo = System.getenv().get("TRUST_STORE");
		if (foo != null) {
			logger.debug("Setting trust store to :" + foo);
			System.setProperty("javax.net.ssl.trustStore", foo);
		} else {
			String path = System.getenv("PATH");
			String[] tokens = path.split(":");
			if (tokens.length == 0) {
				tokens = path.split(";");
			}
			for (int i = 0; i < tokens.length; i++) {
				if (tokens[i].contains("java") && tokens[i].contains("bin")) {
					foo = tokens[1];
				}
			}
			if (foo == null) {
				logger.debug("Unable to locate java home directory. Please set JAVA_HOME" + " Environment variable.");
			} else {

				char temp = File.separatorChar;
				String bar = foo + temp + ".." + temp + "jre" + temp + "lib" + temp + "security" + temp + "cacerts";
				logger.debug("Setting TRUST_STORE to " + bar);
				System.setProperty("javax.net.ssl.trustStore", bar);
			}
		}
		logger.debug("Using java home as: " + foo);

		Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
		// SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory
		// .getDefault();

		return retval;
	}

	private static RecordCapsule processResponse(RecordCapsule input, String response) {
		RecordCapsule retval = input;
		if (response.length() > 0) {
			retval.addDataCapsule(new DataCapsule("SOAP_RESPONSE", response.toString()), false);
		} else {
			retval.addDataCapsule(new DataCapsule("SOAP_RESPONSE", "No response"), false);
		}
		return retval;
	}

	public RecordCapsule processRecord(RecordCapsule input) {
		RecordCapsule retval = input;
		String request = new String(soapRequest);
		if (input != null) {
			for (int i = 0; i < input.getFieldCount(); i++) {
				if (!input.getField(i).isNull()) {
					String token = input.getField(i).getName();
					String value = input.getField(i).getData().toString();
					token = "%" + token + "%";
					request = request.replace(token, value);
				}
			}
			request = request.replace("> ", ">");
			request = request.replace(" <", "<");

			URL u = null;
			try {
				u = new URL(url);
			} catch (MalformedURLException e) {
				logger.error(e.toString() + ":" + url);
			}
			logger.debug("URL Class:" + u);

			HttpsURLConnection c = null;
			StringBuffer outputsb = new StringBuffer();
			try {
				if (u != null) {
					c = (HttpsURLConnection) u.openConnection();
				}
				if (c != null) {
					c.setRequestMethod("POST");

					String userPass = userid + ":" + pwd;
					String base64UserPass = base64Encode(userPass.getBytes());

					c.addRequestProperty("Authorization", "Basic " + base64UserPass);
					c.addRequestProperty("SOAPAction", "Send");
					c.setAllowUserInteraction(true);
					c.setDoInput(true);
					c.setDoOutput(true);

					c.connect();
					PrintWriter pw = null;
					pw = new PrintWriter(new OutputStreamWriter(c.getOutputStream()));
					pw.println(request + "\r\n");
					pw.flush();

					logger.debug("SOAP Request (sent): " + request);

					BufferedReader sw = new BufferedReader(new InputStreamReader(c.getInputStream()));
					String line = null;
					while ((line = sw.readLine()) != null) {
						outputsb.append(line);
					}
					c.disconnect();
					retval = processResponse(retval, outputsb.toString());
				}
			} catch (IOException e1) {
				logger.error(e1.toString());
			}

			logger.debug("SOAP Response: " + outputsb.toString());
		}
		return retval;
	}

	public void setUserid(String userid) {
		this.userid = userid;
		logger.debug("Userid: " + userid);
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
		logger.debug("Password: " + pwd);
	}

	public void setUrl(String url) {
		logger.debug("URL: " + url);
		this.url = url;
	}

	protected String soapRequest = null;

	public String getSoapRequest() {

		return soapRequest;
	}

	public void setSoapRequest(String soap_request) {
		logger.debug("SOAP Request (template): " + soap_request);
		this.soapRequest = soap_request;
	}

	private static String base64Encode(byte[] bytes) {
		return MiscUtil.encodeB64String(new String(bytes));

	}

	protected String userid = null;
	protected String pwd = null;
	protected String url = null;

	// Class specific log4j logger

	// Class specific log4j logger
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DBTableCompare");

	@Override
	public void done() {
		// TODO Auto-generated method stub

	}

}
