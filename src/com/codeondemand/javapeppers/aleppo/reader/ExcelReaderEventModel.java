package com.codeondemand.javapeppers.aleppo.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Observable;

/**
 * This example shows how to use the event API for reading a file.
 */
public class ExcelReaderEventModel extends Observable implements HSSFListener, Runnable {
    private SSTRecord sstrec;

    private FileInputStream fin = null;
    private DocumentInputStream din = null;
    HSSFEventFactory factory = null;

    public void initializeModel(String file, int sheet) {
        // create a new file input stream with the input file specified
        // at the command line

        try {
            fin = new FileInputStream(file);

            // create a new org.apache.poi.poifs.filesystem.Filesystem
            POIFSFileSystem poifs = new POIFSFileSystem(fin);

            // get the Workbook (excel part) stream in a InputStream
            din = poifs.createDocumentInputStream("Workbook");

            // lazy listen for ALL records with the listener shown above
            this.setSheetToListenFor(sheet);

            // create our event factory
            factory = new HSSFEventFactory();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void startProcess() {
        // construct out HSSFRequest object
        HSSFRequest req = new HSSFRequest();

        req.addListenerForAllRecords(this);

        // process our events based on the document input stream
        factory.processEvents(req, din);

        finished();
    }

    public void finished() {
        // once all the events are processed close our file input stream
        try {
            fin.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // and our document input stream (don't want to leak these!)
        din.close();
        finished = true;

    }

    /**
     * This method istens for incoming records and handles them as required.
     *
     * @param record The record that was found while reading.
     */
    public void processRecord(Record record) {

        if (record.getSid() == SSTRecord.sid) {
            sstrec = (SSTRecord) record;
        }

        if (record.getSid() == BOFRecord.sid) {
            if (((BOFRecord) record).getType() == BOFRecord.TYPE_WORKSHEET) {
                sheetcount++;
                rowcount = 0;
            }
        }

        if (slist.contains(sheetcount)) {
            // System.out.println( "sid: "+ record.getSid());
            switch (record.getSid()) {
                // the BOFRecord can represent either the beginning of a sheet or
                // the workbook
                case BOFRecord.sid:
                    BOFRecord bof = (BOFRecord) record;
                    if (bof.getType() == BOFRecord.TYPE_WORKBOOK) {
                        // System.out.println("Encountered workbook");
                        // assigned to the class level member
                    } else if (bof.getType() == BOFRecord.TYPE_WORKSHEET) {
                        System.out.println("Encountered sheet reference");
                    }
                    break;
                case BoundSheetRecord.sid:
                    BoundSheetRecord bsr = (BoundSheetRecord) record;
                    // System.out.println("New sheet named: " + bsr.getSheetname());
                    logger.debug("New sheet named: " + bsr.getSheetname());
                    break;

                case BlankRecord.sid:
                    BlankRecord blkrec = (BlankRecord) record;
                    currentColumn = blkrec.getColumn();
                    if (currentColumn < currentRow.length) {
                        currentRow[currentColumn] = "";
                    }
                    break;
                case BoolErrRecord.sid:
                    BoolErrRecord boolrec = (BoolErrRecord) record;
                    currentColumn = boolrec.getColumn();
                    break;

                case RowRecord.sid:
                    RowRecord rowrec = (RowRecord) record;
                    if (currentRow == null) {
                        if (lastColumn < 0) {
                            lastColumn = rowrec.getLastCol();
                        }
                        currentRow = new Object[lastColumn];
                    }
                    break;
                case MulBlankRecord.sid:
                    MulBlankRecord mblkrec = (MulBlankRecord) record;
                    currentColumn = mblkrec.getLastColumn();
                    break;
                case NumberRecord.sid:
                    NumberRecord numrec = (NumberRecord) record;
                    if (numrec.getColumn() < currentRow.length) {
                        currentRow[numrec.getColumn()] = new Double(numrec.getValue()).toString();
                        //System.out.println("adding " + numrec.getValue());
                        data_record = true;
                        currentColumn = numrec.getColumn();
                    }
                    break;
                // SSTRecords store a array of unique strings used in Excel.
                case SSTRecord.sid:
                    sstrec = (SSTRecord) record;
                    // for (int k = 0; k < sstrec.getNumUniqueStrings(); k++) {
                    // System.out.println("String table value " + k + " = "
                    // + sstrec.getString(k));
                    // }
                    break;
                case LabelSSTRecord.sid:
                    LabelSSTRecord lrec = (LabelSSTRecord) record;
                    currentColumn = lrec.getColumn();
                    if (currentColumn <= currentRow.length - 1) {
                        currentRow[lrec.getColumn()] = sstrec.getString(lrec.getSSTIndex());
                        data_record = true;
                        //System.out.println("adding "
                        //+ sstrec.getString(lrec.getSSTIndex()));
                    }
                    break;
            }
            if (data_record && (currentColumn >= lastColumn - 1)) {
                if (rowcount >= startRow) {
                    continue_flg = false;

                    retRow = new Object[currentRow.length];
                    for (int i = 0; i < currentRow.length; i++) {
                        if (currentRow[i] != null) {
                            retRow[i] = currentRow[i].toString();
                        }
                        currentRow[i] = null;
                    }

                    //System.out.println("row: " + rowcount + " start: " + startRow);
                    this.setChanged();
                    notifyObservers(true);
                    currentColumn = 0;
                    data_record = false;
                    while (!continue_flg) {
                        Thread.yield();
                    }
                }
                rowcount++;
            }
        }
    }

    public void setSheetToListenFor(int num) {
        slist.add(num);
    }

    public void setContinue(boolean b) {
        synchronized (continue_lock) {
            continue_flg = b;
        }
    }

    public void setStartRow(int row) {
        startRow = row;
    }

    public void setLastColumn(int col) {
        lastColumn = col;
    }

    private ArrayList<Integer> slist = new ArrayList<>();
    private int sheetcount = 0;
    private int startRow = 0;
    private int rowcount = 0;
    private Object[] currentRow = null;
    private Object[] retRow = null;
    private int currentColumn = 0;
    private int lastColumn = -1;
    private Boolean continue_flg = true;
    private Boolean finished = false;
    private boolean data_record = false;
    private final Object finish_lock = new Object();
    private final Object continue_lock = new Object();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("ExcelReaderEventModel");

    public void run() {
        startProcess();
        synchronized (finish_lock) {
            while (!finished) {
                Thread.yield();
            }
        }
        setChanged();
        notifyObservers(Boolean.FALSE);
        // TODO Auto-generated method stub
    }

    public Object[] getCurrentRow() {
        return retRow;
    }
}
