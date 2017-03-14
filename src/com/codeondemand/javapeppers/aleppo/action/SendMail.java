/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.action;

import java.io.File;
import java.util.StringTokenizer;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;


import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.habanero.util.misc.GetFileList;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import com.codeondemand.javapeppers.poblano.email.SMTPEmailAgent;

public class SendMail extends RecordProcessor {

	@Override
	public RecordCapsule processRecord(RecordCapsule input) {
		RecordCapsule retval = input;
		if (!initialized) {
			initialized = doInitialization();
		}
		if (initialized) {
			try {

				if (pmap.containsKey("mail.addresses.sendto") && pmap.containsKey("mail.address.sentfrom")) {
					Message msg = new MimeMessage(session);
					msg.setFrom(new InternetAddress((String) pmap.get("mail.address.sentfrom")));
					if (pmap.containsKey("mail.subject")) {
						msg.setSubject((String) pmap.get("mail.subject"));
					} else {
						msg.setSubject("NO SUBJECT SUPPLIED TO SendMail");
					}
					String addrs = (String) pmap.get("mail.addresses.sendto");
					// System.out.println("MAIL TO:"+addrs);
					StringTokenizer strtoks = new StringTokenizer(addrs, "|");

					while (strtoks.hasMoreTokens()) {
						msg.addRecipient(Message.RecipientType.TO, new InternetAddress(strtoks.nextToken()));
					}

					Multipart multipart = new MimeMultipart();

					BodyPart contentBodyPart = createMessageContent(retval);

					if (pmap.containsKey("mail.send_capsule_data")
							&& Boolean.parseBoolean((String) pmap.get("mail.send_capsule_data"))) {
						//TO-DO: Add some code

					}

					if (pmap.containsKey("mail.attachment")) {
						// Part two is attachment
						BodyPart messageBodyPart = new MimeBodyPart();
						DataSource source = new FileDataSource((String) pmap.get("mail.attachment"));
						messageBodyPart.setDataHandler(new DataHandler(source));
						String temp = (String) pmap.get("mail.attachment");
						temp = temp.substring(temp.lastIndexOf(File.separator) + 1);
						messageBodyPart.setFileName(temp);
						// System.out.println("ATTACHTMENT:"+pmap.get("mail.attachment"));
						;
						multipart.addBodyPart(messageBodyPart);
					} else if (pmap.containsKey("mail.attachment.pattern") && pmap.containsKey("mail.attachment.dir")) {
						GetFileList t = new GetFileList((String) pmap.get("mail.attachment.dir"),
								(String) pmap.get("mail.attachment.pattern"));

						java.util.Iterator<String> it = t.getList();
						while (it.hasNext()) {
							BodyPart messageBodyPart = new MimeBodyPart();
							String filename = it.next().toString();
							DataSource source = new FileDataSource(filename);
							messageBodyPart.setDataHandler(new DataHandler(source));
							String shortname = filename.substring(filename.lastIndexOf(File.separator) + 1);

							messageBodyPart.setFileName(shortname);
							multipart.addBodyPart(messageBodyPart);
						}

					}
					multipart.addBodyPart(contentBodyPart);
					// Put parts in message
					msg.setContent(multipart);

					agent.sendMail(msg);
				}

			} catch (MessagingException e) {
				retval = null;
				logger.error(e.toString());
			}

		} else {
			retval = null;
		}

		return retval;
	}

	protected BodyPart createMessageContent(RecordCapsule retval) {
		MimeBodyPart messageBodyPart = new MimeBodyPart();
		try {
			if (retval.getField("message") != null && !retval.getField("message").isNull()) {
				messageBodyPart.setText(retval.getField("message").getData().toString());
			} else if (pmap.containsKey("message")) {
				messageBodyPart.setText(pmap.get("message").toString());
			} else {
				messageBodyPart.setText("no message content found.");

			}
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return messageBodyPart;
	}

	@Override
	public boolean doInitialization() {
		boolean retval = false;
		if (pmap.containsKey("mail.smtp.host") && pmap.containsKey("mail.uid") && pmap.containsKey("mail.pwd")) {

			agent = new SMTPEmailAgent();
			agent.initialize((String) pmap.get("mail.smtp.host"), (String) pmap.get("mail.uid"),
					MiscUtil.decodeB64String((String) pmap.get("mail.pwd")));
			session = agent.getSession();
			if (session != null) {
				retval = true;
			}
		}
		return retval;
	}

	private SMTPEmailAgent agent = null;
	private Session session = null;
	private boolean initialized = false;
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SendMail");
	
	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}
}
