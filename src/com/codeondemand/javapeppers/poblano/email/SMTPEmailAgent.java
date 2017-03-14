/**
 * 
 */
package com.codeondemand.javapeppers.poblano.email;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;


import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

public class SMTPEmailAgent {

	/*
	 * This will terminate the current connection to the SMTP server
	 * 
	 */
	public void finished(){
		if( tr != null && tr.isConnected()){
			try {
				tr.close();
			} catch (MessagingException e) {
				logger.error(e.toString());
			}
		}
	}
	
	/*
	 * Now actually send the message.
	 */
	public boolean sendMail( Message msg){
		boolean retval = false;
		try {
			if( tr != null && tr.isConnected() ){
				tr.sendMessage(msg, msg.getAllRecipients());
				retval = true;				
			}
		} catch (MessagingException e) {
			logger.error(e.toString());
		}		
		return retval;
	}
	
	public Session getSession(){
		return s;
	}
	
	public boolean initialize( Properties props){
		boolean retval = false;
		if( props != null ){
			String host = props.getProperty("mail.smtp.host");
			String uid  = props.getProperty("mail.uid");
			String pwd  = props.getProperty("mail.pwd");
			if( Boolean.parseBoolean(props.getProperty("mail.encrypted.pwd"))){
				pwd = MiscUtil.decodeB64String(pwd);
			}
			if( host != null && uid != null & pwd != null ){
				try {
					s = Session.getInstance(props);
					tr = s.getTransport("smtp");
					tr.connect(host, uid, pwd);
					retval = true;
				} catch (MessagingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}
		return retval;
	}
	
	public boolean initialize(String host, String uid, String pwd) {
		boolean retval = false;
		
		try {
			if (host != null && uid != null && pwd != null) {
				Properties props = (Properties) System.getProperties().clone();
				props.put("mail.smtp.host", host);
				Session s = Session.getInstance(props);
				tr = s.getTransport("smtp");
				tr.connect(host, uid, pwd);
				retval = true;
			}
		} catch (MessagingException e) {
			logger.error(e.toString());
		}
		return retval;
	}

	private Session s = null;
	private Transport tr = null;
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("SMTPEmailAgent");
}
