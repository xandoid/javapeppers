package com.codeondemand.javapeppers.poblano.email;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;
import javax.mail.*;
import javax.mail.internet.*;

public class SendMail {

    public void sendMail(String propfile) {

        if (initialize(propfile)) {
            Message msg = buildMsg();
            agent.sendMail(msg);
            agent.finished();
        } else {
            logger.error("Unable to send mail:\n");
            logger.error("\tSyntax: Sendmail <property file>\n\n");
        }
    }

    private static BodyPart createMessageContent(String msg) {
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        try {
            messageBodyPart.setText(msg);
        } catch (MessagingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return messageBodyPart;
    }

    private Message buildMsg() {
        Message msg = null;
        Session s = agent.getSession();
        if (s != null) {
            msg = new MimeMessage(s);
            try {
                msg.setFrom(new InternetAddress(mailFrom));
                if (replyTo != null) {
                    Address[] a = new Address[1];
                    a[0] = new InternetAddress(replyTo);
                    msg.setReplyTo(a);
                }
                StringTokenizer stok = new StringTokenizer(mailTo, "|");
                while (stok.hasMoreTokens()) {
                    msg.addRecipient(Message.RecipientType.TO, new InternetAddress(stok.nextToken()));
                }

                msg.setSubject(subject);
                StringBuffer sb = new StringBuffer();
                if (mailBodyTxt != null) {
                    sb.append(mailBodyTxt + "\n");
                }
                if (mailBodyFile != null) {
                    BufferedReader brdr = null;
                    try {
                        brdr = new BufferedReader(new FileReader(mailBodyFile));
                        while (brdr.ready()) {
                            String temp = brdr.readLine();
                            if (temp != null) {
                                String line = temp.trim();
                                line = subTokens(line);
                                sb.append(line + "\n");
                            }
                        }
                        brdr.close();
                    } catch (FileNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                BodyPart messageBodyPart = createMessageContent(sb.toString());
                Multipart multipart = new MimeMultipart();
                multipart.addBodyPart(messageBodyPart);

                if (attachment != null) {
                    messageBodyPart = new MimeBodyPart();
                    DataSource source = new FileDataSource(attachment);
                    messageBodyPart.setDataHandler(new DataHandler(source));
                    messageBodyPart.setFileName(attachment);
                    multipart.addBodyPart(messageBodyPart);
                }

                // Put parts in message
                msg.setContent(multipart);

            } catch (AddressException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (MessagingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return msg;

    }

    private boolean initialize(String propfile) {
        boolean retval = true;
        props = MiscUtil.loadXMLPropertiesFile(propfile);

        if (props == null) {
            retval = false;
        } else {
            agent = new SMTPEmailAgent();
            initialized = agent.initialize(props);

            mailTo = props.getProperty("mailto");
            subject = mapEnvTokens(props.getProperty("mail.subject"));
            subject = subTokens(subject);
            mailFrom = props.getProperty("mailfrom");
            attachment = getStringProperty(props, "attachment");
            mailBodyFile = getStringProperty(props, "mail.body.file");
            mailBodyTxt = getStringProperty(props, "mail.body.txt");
            replyTo = props.getProperty("replyTo");
        }

        retval = initialized;
        return retval;
    }

    private static String getStringProperty(Properties p, String prop) {
        return subTokens(p.getProperty(prop));
    }

    private String mapEnvTokens(String input) {
        String retval = new String(input);
        if (emap == null) {
            emap = System.getProperties();
        }
        if (emap.containsKey("STATUS")) {
            retval = MiscUtil.replaceToken(retval, "%STATUS%", emap.getProperty("STATUS"));
        }
        return retval;
    }

    private static String subTokens(String input) {
        String retval = input;
        if (retval != null) {
            retval = MiscUtil.replaceToken(retval, "%DT%", MiscUtil.getCurrentDateString());
            retval = MiscUtil.replaceToken(retval, "%TS%", MiscUtil.getCurrentTimeString());
            retval = MiscUtil.replaceToken(retval, "%YESTERDAY%", MiscUtil.getOffsetDateString(-1));
        }
        return retval;
    }

    private SMTPEmailAgent agent = null;

    private Properties props = null;
    private String mailTo = null;
    private String mailFrom = null;
    private String replyTo = null;
    private String subject = null;
    private String attachment = null;
    private String mailBodyFile = null;
    private String mailBodyTxt = null;

    private Properties emap = null;

    private boolean initialized = false;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SendMail");

    public static void main(String[] args) {
        if ((args.length > 0) && (args[0] != null)) {
            SendMail foo = new SendMail();
            foo.sendMail(args[0]);
        }

    }

}
