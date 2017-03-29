package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class FieldDecoder extends RecordTransform {

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {

        if (field != null && input.getField(field) != null && !input.getField(field).isNull()) {
            String foo = input.getField(field).getData().toString();
            String bar = MiscUtil.decodeSimpleB64String(foo);
            StringBuilder sb = new StringBuilder();
            ByteArrayInputStream bais = new ByteArrayInputStream(bar.getBytes());
            try {
                int sChunk = 16378;
                byte[] buffer = new byte[sChunk];
                GZIPInputStream gzis = new GZIPInputStream(bais);
                while (gzis.read(buffer, 0, sChunk) != -1) {
                    String s = new String(buffer);
                    sb.append(s);
                }

                input.getField(field).setData(sb.toString());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return input;
    }

    @Override
    public boolean doInitialization() {
        boolean retval = true;
        if (pmap.containsKey("field")) {
            field = (String) pmap.get("field");
        } else {
            retval = false;
        }
//		if (pmap.containsKey("codec")) {
//			codec = (String) pmap.get("codec");
//		}
        return retval;
    }

    @Override
    public RecordCapsule doTransform(RecordCapsule input) {
        // TODO Auto-generated method stub
        return null;
    }

    private String field = null;
//	private String codec = null;

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }

}
