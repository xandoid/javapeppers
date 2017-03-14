/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.ui;

import java.awt.Container;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;


import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.AleppoConstants;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.reader.UISourceReader;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;


public class UIPrompt extends UISourceReader implements ActionListener {

	public UIPrompt() {
	}

	@Override
	public boolean initialize(RecordCapsule format) {
		rc = format;
		frame = new JFrame(rc.getName());
		Container p = frame.getContentPane();
		int fieldcount = rc.getFieldCount();
		GridLayout entry_layout = new GridLayout(fieldcount+2,2);
		JPanel entry_panel = new JPanel(entry_layout);
		datafields = new JTextField[fieldcount];
		entry_panel.add(new JLabel("Press OK to run"));
		entry_panel.add(new JLabel());
		for( int i = 0; i < fieldcount ; i++){
			entry_panel.add( new JLabel((String)rc.getField(i).getMetaData("display_name")));
			datafields[i] = new JTextField();
			if( rc.getField(i).getMetaData("length") != null){
				datafields[i].setColumns(Integer.parseInt(rc.getField(i).getMetaData("length").toString()));				
			}else{
				datafields[i].setColumns(40);
			}
			entry_panel.add(datafields[i]);
		}	
		entry_panel.add(okbtn);
		entry_panel.add(exitbtn);
		GridLayout l = new GridLayout(1, 1);
		p.setLayout(l);
		p.add(entry_panel);
		okbtn.addActionListener(this);
		exitbtn.addActionListener(this);
		frame.pack();
		frame.setLocation(40,40);
		frame.setVisible(true);
		return true;
	}
	
	private RecordCapsule buildData() {
		RecordCapsule retval = null;
		if (!finished) {
			//retval = new RecordCapsule(rc.getName(), null);
			retval = new RecordCapsule(rc.getName(),null);
			for( int i = 0; i < rc.getFieldCount(); i++){
				DataCapsule dc = new DataCapsule(rc.getField(i).getName(),null);
				if( rc.getField(i).getMetaData(AleppoConstants.ALEPPO_DC_MDATA_TYPE_KEY)  != null){
					String typeName = rc.getField(i).getMetaData(AleppoConstants.ALEPPO_DC_MDATA_TYPE_KEY).toString();
					String value = datafields[i].getText().trim();
					if( value.length() != 0){
						if( typeName.equalsIgnoreCase("INTEGER")){
							dc.setData(Integer.parseInt(datafields[i].getText()));
						}else if( typeName.equalsIgnoreCase("FLOAT")){
							dc.setData(Float.parseFloat(datafields[i].getText()));
						}else if( typeName.equalsIgnoreCase("DOUBLE")){
							dc.setData(Double.parseDouble(datafields[i].getText()));
						}else if( typeName.equalsIgnoreCase("BOOLEAN")){
							dc.setData(Boolean.parseBoolean(datafields[i].getText()));
						}else if ( typeName.equalsIgnoreCase("ARRAY")){
							dc.setData(parseArray(datafields[i].getText(),"|"));
						}	else{
							dc.setData(datafields[i].getText().trim());																		
						}						
					}
				}else{
						dc.setData(null);
				}
				retval.addDataCapsule(dc, false);			
			}
			reset();
			ready = false;
		} else {
			frame.dispose();
			logger.debug("Disposing window");
		}
		return retval;
	}

	public boolean reset() {
		for( int i = 0 ; i < datafields.length; i++){
			datafields[i].setText(null);			
		}
		return true;
	}


	public synchronized void actionPerformed(ActionEvent e) {
		if (e.getSource() == exitbtn) {
			logger.debug( "exit action performed");
			reset();
			finished = true;
			ready = true;
		} else if(e.getSource() == okbtn ){
			logger.debug( "ok action performed");
			finished = false;
			ready = true;
		}
	}

	public boolean close() {
		frame.dispose();
		return true;
	}

	public Object read() {
		Object retval = null;
		while (!ready ) {
			try {
				Thread.sleep(10);
				Thread.yield();
			} catch (InterruptedException e) {
				logger.error(e.toString());
			}
		}
		if( !finished){
			retval= buildData();
			logger.debug( "Returning:"+retval);			
		}else{
			retval = null;
		}
		return retval;
	}
	
	@Override
	public boolean doInitialization() {
		return true;
	}
	
	
	private static Object[] parseArray(String value, String delim){
		ArrayList<String> retval = MiscUtil.StringToList(value, delim);
		return retval.toArray();
	}
	//private JTextField data = new JTextField(50);
	private JButton okbtn = new JButton("OK");
	private JButton exitbtn = new JButton("EXIT");
	private JFrame frame = null;
	private boolean finished = false;
	private boolean ready = false;
	private JTextField[] datafields = null;
	private RecordCapsule rc = null;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("UIPrompt");



}
