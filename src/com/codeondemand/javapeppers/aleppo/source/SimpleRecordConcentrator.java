package com.codeondemand.javapeppers.aleppo.source;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

import java.util.Properties;

public class SimpleRecordConcentrator extends SourceConcentrator {

    @Override
    public RecordCapsule accumulateRecord(RecordCapsule newrec, RecordCapsule oldrec) {
        RecordCapsule retval = null;
        if (currentRecord == null) {
            currentRecord = createNewRecord(newrec);
        } else {
            for (int n = 0; n < currentRecord.getFieldCount(); n++) {
                if (!currentRecord.isKey(currentRecord.getField(n).getName())) {
                    String name = currentRecord.getField(n).getName();
                    Object o1 = currentRecord.getField(name).getData();
                    Object o2 = newrec.getField(name).getData();
                    if (o1 != null && o2 != null) {
                        Object temp = null;
                        if (o2.getClass() == o1.getClass()) {
                            if (o1 instanceof Integer) {
                                temp = (Integer) o1 + (Integer) o2;
                            } else if (o1 instanceof Double) {
                                temp = (Double) o1 + (Double) o2;
                            } else if (o1 instanceof Float) {
                                temp = (Float) o1 + (Float) o2;
                            }
                            currentRecord.setData(temp);
                        }
                    }
                }
            }
        }
        return retval;
    }

    @Override
    public RecordCapsule buildHeaderObject(RecordCapsule headerRecord) {
        // TODO Auto-generated method stub
        return headerRecord;
    }

    @Override
    public RecordCapsule buildRecord(RecordCapsule currentRecord) {
        return currentRecord;
    }

    private static RecordCapsule createNewRecord(RecordCapsule rec) {
        RecordCapsule retval = new RecordCapsule(rec.getName(), null);
        for (int n = 0; n < rec.getFieldCount(); n++) {
            retval.addDataCapsule(rec.getField(n).cloneDC(), rec.isKey(rec.getField(n).getName()));
        }
        return retval;
    }

    public void setProperties(Properties props) {
        this.props = props;
    }

    protected Properties props = null;

    RecordCapsule currentRecord = null;

}
