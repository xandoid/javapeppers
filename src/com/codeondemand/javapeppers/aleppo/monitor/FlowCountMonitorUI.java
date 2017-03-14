package com.codeondemand.javapeppers.aleppo.monitor;

import java.awt.Container;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

public class FlowCountMonitorUI extends FlowCountMonitor implements
		ActionListener {

	public boolean initialize(RecordCapsule rc) {
		frame = new JFrame("Flow Counter");
		Container p = frame.getContentPane();
		int fieldcount = 3;
		GridLayout entry_layout = new GridLayout(fieldcount + 2, 2);
		JPanel entry_panel = new JPanel(entry_layout);
		datafields = new JTextField[fieldcount];
		entry_panel.add(new JLabel());
		entry_panel.add(new JLabel());
		entry_panel.add(new JLabel("Count     "));
		datafields[0] = new JTextField();
		datafields[0].setColumns(15);
		entry_panel.add(datafields[0]);

		entry_panel.add(new JLabel("Tot Count / sec"));
		datafields[1] = new JTextField();
		datafields[1].setColumns(15);
		entry_panel.add(datafields[1]);
		entry_panel.add(new JLabel("Interval Count / sec"));
		datafields[2] = new JTextField();
		datafields[2].setColumns(15);
		entry_panel.add(datafields[2]);
		//entry_panel.add(exitbtn);
		GridLayout l = new GridLayout(1, 1);
		p.setLayout(l);
		p.add(entry_panel);

		exitbtn.addActionListener(this);
		frame.pack();
		frame.setLocation(40, 40);
		frame.setVisible(true);

		return super.initialize(rc);
	}

	protected boolean doMonitor(RecordCapsule rc){
		if( !initialized ){
			initialize(rc);
		}
		return super.doMonitor(rc);
	}
	protected void showCount(String name, Long cnt, Long totPerSec, Long intervalPerSec) {

			datafields[0].setText(cnt.toString());
			datafields[1].setText(totPerSec.toString());
			datafields[2].setText(intervalPerSec.toString());

	}

	public synchronized void actionPerformed(ActionEvent e) {
		if (e.getSource() == exitbtn) {
			logger.debug("reset action performed");
			reset();
		}
	}

	// private JTextField data = new JTextField(50);
	private JButton exitbtn = new JButton("RESET");
	private JFrame frame = null;
	private JTextField[] datafields = null;
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FlowCountMonitor");
}
