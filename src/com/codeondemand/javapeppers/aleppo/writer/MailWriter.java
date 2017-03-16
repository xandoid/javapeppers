package com.codeondemand.javapeppers.aleppo.writer;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import com.codeondemand.javapeppers.poblano.email.SMTPEmailAgent;
import org.apache.logging.log4j.LogManager;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Properties;
import java.util.StringTokenizer;

public class MailWriter extends DestinationWriter {

    public boolean close() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean reset() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean write(Object data) {
        boolean retval = false;
        if (!initialized && data instanceof RecordCapsule) {
            initialized = doInitialization((RecordCapsule) data);
            rec = (RecordCapsule) data;
        }
        if (initialized) {
            try {

                if (props.containsKey("mail.addresses.sendto") && props.containsKey("mail.address.sentfrom")) {
                    Message msg = new MimeMessage(session);
                    msg.setFrom(new InternetAddress((String) props.get("mail.address.sentfrom")));
                    if (props.containsKey("mail.subject")) {
                        msg.setSubject((String) props.get("mail.subject"));
                    } else {
                        msg.setSubject("NO SUBJECT SUPPLIED TO SendMail");
                    }
                    String addrs = (String) props.get("mail.addresses.sendto");
                    StringTokenizer strtoks = new StringTokenizer(addrs, "|");

                    if (strtoks.hasMoreTokens()) {
                        msg.addRecipient(Message.RecipientType.TO, new InternetAddress(strtoks.nextToken()));
                    }

                    BodyPart messageBodyPart = createMessageContent(rec);

                    Multipart multipart = new MimeMultipart();
                    multipart.addBodyPart(messageBodyPart);

                    if (props.containsKey("mail.send_capsule_data") && Boolean.parseBoolean((String) props.get("mail.send_capsule_data"))) {
                        // TO-DO: ?

                    }

                    if (props.containsKey("mail.attachment")) {
                        // Part two is attachment
                        messageBodyPart = new MimeBodyPart();
                        DataSource source = new FileDataSource((String) props.get("mail.attachment"));
                        messageBodyPart.setDataHandler(new DataHandler(source));
                        messageBodyPart.setFileName((String) props.get("mail.attachment"));
                        multipart.addBodyPart(messageBodyPart);
                    }

                    // Put parts in message
                    msg.setContent(multipart);

                    agent.sendMail(msg);
                    retval = true;
                }

            } catch (MessagingException e) {
                logger.error(e.toString());
            }

        }
        return retval;
    }

    protected BodyPart createMessageContent(RecordCapsule retval) {
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        try {
            if (retval.getField("message") != null && !retval.getField("message").isNull()) {
                messageBodyPart.setText(retval.getField("message").getData().toString());
            } else {
                messageBodyPart.setText("no message content found.");
            }
        } catch (MessagingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return messageBodyPart;
    }

    public boolean doInitialization(RecordCapsule rec) {
        boolean retval = false;
        if (rec.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY) != null) {
            props = (Properties) rec.getMetaData(AleppoConstants.ALEPPO_DC_MDATA_PROPERTIES_KEY);
            if (props != null && props.containsKey("mail.smtp.host") && props.containsKey("mail.uid") && props.containsKey("mail.pwd")) {

                agent = new SMTPEmailAgent();
                agent.initialize((String) props.get("mail.smtp.host"), (String) props.get("mail.uid"), MiscUtil.decodeB64String((String) props.get("mail.pwd")));
                session = agent.getSession();
                if (session != null) {
                    retval = true;
                }
            }
        }
        return retval;
    }

    private SMTPEmailAgent agent = null;
    private Session session = null;
    private RecordCapsule rec = null;
    private boolean initialized = false;
    //Properties props = null;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MailWriter");

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }
}
