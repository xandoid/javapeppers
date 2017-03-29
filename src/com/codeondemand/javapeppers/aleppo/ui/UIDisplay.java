/**
 *
 */
package com.codeondemand.javapeppers.aleppo.ui;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.writer.UIRecordWriter;
import org.apache.logging.log4j.LogManager;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class UIDisplay extends UIRecordWriter implements ActionListener {

    @Override
    /**
     * The initialize method will create a simple GUI screen that will
     * display the values of the records coming through.
     */ public boolean initialize(RecordCapsule format) {
        rc = format;
        frame = new JFrame(rc.getName());
        Container p = frame.getContentPane();
        int fieldcount = rc.getFieldCount();

        GridLayout label_layout = new GridLayout(fieldcount, 1);
        JPanel label_panel = new JPanel(label_layout);

        GridBagLayout data_layout = new GridBagLayout();
        GridBagConstraints data_constraints = new GridBagConstraints();
        data_constraints.gridx = 0;
        data_constraints.anchor = GridBagConstraints.WEST;
        JPanel data_panel = new JPanel(data_layout);


        datafields = new JTextField[fieldcount];

        for (int i = 0; i < fieldcount; i++) {
            DataCapsule dc = rc.getField(i);

            // Create a label with either the specified display name or
            // the actual name stored by the DataCapsule. Add this to
            // the entry panel. Keep track of the maximum string length
            JLabel temp = new JLabel();
            if (dc.getMetaData("display_name") != null) {
                temp.setText(dc.getMetaData("display_name").toString());
            } else {
                temp.setText(dc.getName());
            }

            label_panel.add(temp);

            datafields[i] = new JTextField();
            datafields[i].setEditable(false);
            if (dc.getMetaData("length") != null) {
                int foo = new Integer(dc.getMetaData("length").toString());
                foo = Math.min(foo, 50);
                datafields[i].setColumns(foo);
            } else {
                datafields[i].setColumns(40);
            }

            datafields[i].setName(rc.getField(i).getName());
            data_layout.setConstraints(datafields[i], data_constraints);
            data_panel.add(datafields[i]);
        }

        if (rc.getMetaData("xlocation") != null && rc.getMetaData("ylocation") != null) {
            int x = new Integer(rc.getMetaData("xlocation").toString());
            int y = new Integer(rc.getMetaData("ylocation").toString());
            frame.setLocation(x, y);
        } else {
            frame.setLocation(200, 200);
        }

        GridBagConstraints c = new GridBagConstraints();

        GridBagLayout l = new GridBagLayout();
        p.setLayout(l);
        c.fill = GridBagConstraints.VERTICAL;
        c.ipadx = 10;
        c.insets = new Insets(5, 10, 5, 0);
        l.setConstraints(label_panel, c);
        l.setConstraints(data_panel, c);

        GridLayout btn_layout = new GridLayout(1, 5);
        exitbtn.addActionListener(this);
        nextbtn.addActionListener(this);
        p.add(label_panel);
        p.add(data_panel);

        JPanel btn_panel = new JPanel(btn_layout);
        btn_panel.add(new JLabel(""));
        btn_panel.add(exitbtn);
        btn_panel.add(new JLabel(""));
        btn_panel.add(nextbtn);
        btn_panel.add(new JLabel(""));
        c.gridx = 1;
        l.setConstraints(btn_panel, c);
        p.add(btn_panel);

        frame.pack();
        frame.setVisible(true);
        return true;
    }

    public boolean close() {
        boolean retval = true;
        frame.dispose();
        return retval;
    }

    public boolean reset() {
        for (JTextField datafield : datafields) {
            datafield.setText(null);
        }
        return true;
    }

    public boolean write(Object record) {
        boolean retval = true;
        if (!initialized && record instanceof RecordCapsule) {
            initialized = initialize((RecordCapsule) record);
        } else {
            retval = false;
        }
        if (initialized && !finished) {
            if (record instanceof RecordCapsule) {
                RecordCapsule temp = (RecordCapsule) record;
                for (int i = 0; i < datafields.length; i++) {
                    logger.debug("fetching name for data field " + i);
                    logger.debug("Found: " + datafields[i].getName());
                    DataCapsule dc = temp.getField(datafields[i].getName().trim());
                    if (dc != null && !dc.isNull()) {
                        datafields[i].setText(dc.getData().toString());
                    } else {
                        datafields[i].setText(null);
                    }
                }
                ready = false;
                while (!ready) {
                    Thread.yield();
                }
                retval = !finished;
            }
        }
        return retval;
    }

    private JButton nextbtn = new JButton("NEXT");
    private JButton exitbtn = new JButton("EXIT");
    private JFrame frame = null;
    private boolean finished = false;
    private boolean ready = false;
    private JTextField[] datafields = null;
    private boolean initialized = false;
    private RecordCapsule rc = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("UIDisplay");

    public synchronized void actionPerformed(ActionEvent e) {
        if (e.getSource() == exitbtn) {
            finished = true;
            ready = true;
            notify();
            close();
        }
        if (e.getSource() == nextbtn) {
            finished = false;
            ready = true;
            notify();
        }
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

}
