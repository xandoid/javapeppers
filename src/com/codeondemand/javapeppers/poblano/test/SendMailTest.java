package com.codeondemand.javapeppers.poblano.test;

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

import com.codeondemand.javapeppers.poblano.email.SMTPEmailAgent;

public class SendMailTest {

	public SendMailTest() {
	}

	public void run() {
		;
		if (!initialized) {
			initialized = doInitialization();
		}
		if (initialized) {
			try {

				Message msg = new MimeMessage(session);
				msg.setFrom(new InternetAddress("gary_anderson@cz.javapeppers.com"));
				//msg.setFrom(new InternetAddress("gfa@codeondemand.com"));
				msg.setSubject("test message via javapeppers smtp");
				msg.addRecipient(Message.RecipientType.TO, new InternetAddress(
						"gfa"));
				msg.addRecipient(Message.RecipientType.TO, new InternetAddress(
				"gary_anderson@cz.javapeppers.com"));


				BodyPart messageBodyPart = createMessageContent("This is only a test.");

				Multipart multipart = new MimeMultipart();
				multipart.addBodyPart(messageBodyPart);
				
				messageBodyPart = new MimeBodyPart();
				DataSource source = new FileDataSource("./resources/sendmail_test.xml");
				messageBodyPart.setDataHandler(new DataHandler(source));
				messageBodyPart.setFileName("sendmail_test.xml");
				multipart.addBodyPart(messageBodyPart);

				// Put parts in message
				msg.setContent(multipart);

				agent.sendMail(msg);
			} catch (MessagingException e) {
				logger.error(e.toString());
			}
		}
	}

	protected BodyPart createMessageContent(String msg) {
		MimeBodyPart messageBodyPart = new MimeBodyPart();
		try {
			messageBodyPart.setText(msg);
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return messageBodyPart;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SendMailTest foo = new SendMailTest();
		foo.run();
	}

	private boolean doInitialization() {
		boolean retval = false;
		agent = new SMTPEmailAgent();
		boolean test = agent.initialize("9.149.104.94", "gary_anderson@cz.javapeppers.com", "Chack0va");
		session =agent.getSession();
		//session = agent.initialize("smtp.fatcow.com", "gfa@codeondemand.com", "noggins");
		if (test ) {
			retval = true;
		}
		return retval;
	}

	private SMTPEmailAgent agent = null;
	private Session session = null;
	private boolean initialized = false;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SendMailTest");
}
