package com.codeondemand.javapeppers.aleppo.parser;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import org.apache.logging.log4j.LogManager;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class JSONParser extends NullRecordParser {

    @Override
    public RecordCapsule parseRecord(Object input) {
        if (!initialized) {
            initialized = doInitialization();
        }
        RecordCapsule retval = new RecordCapsule("Record_0", false);

        if (input instanceof String) {
            logger.debug(input.toString());
            logger.debug(fieldList.toString());
            JsonObject message_obj = Json.createReader(new StringReader((String) input)).readObject();
            for (String foo : fieldList) {
                if (message_obj.containsKey(foo)) {
                    String value = message_obj.getJsonString(foo).getString();
                    retval.addDataCapsule(new DataCapsule(foo, value), false);
                    // if( bar.getValueType() == ValueType.STRING ){
                    // DataCapsule dc = new DataCapsule(foo,value);
                    // retval.addDataCapsule(dc, false);
                    // }else if( bar.getValueType() == ValueType.NUMBER){
                    // if( bar.getJsonNumber(foo).isIntegral() ){
                    // int value =
                    // bar.getJsonNumber(foo).bigIntegerValue().intValue();
                    // DataCapsule dc = new DataCapsule(foo,value);
                    // retval.addDataCapsule(dc, false);
                    // }
                    // }
                } else {
                    logger.error("Specified field: " + foo + " not found in message.");
                }
            }
        }

        //System.out.println(retval.toString());
        return retval;
    }

    public boolean doInitialization() {
        // TODO Auto-generated method stub
        if (pmap.containsKey("fields")) {
            StringTokenizer stok = new StringTokenizer((String) pmap.get("fields"), "|");
            while (stok.hasMoreTokens()) {
                fieldList.add(stok.nextToken());
            }
        }
        if (pmap.containsKey("doTS")) {
            doTS = Boolean.parseBoolean((String) pmap.get("doTS"));
        }

        if (pmap.containsKey("outputField")) {
            outputField = (String) pmap.get("outputField");
        }
        initialized = true;
        return true;
    }


    private ArrayList<String> fieldList = new ArrayList<String>();
    private String outputField = null;
    private boolean doTS = false;
    private boolean initialized = false;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("JSONParser");

    // public RecordCapsule parseRecord(Object input) {
    // RecordCapsule retval = null;
    // if (input instanceof String) {
    // // Find the first occurrence of "{" and the last occurence of "}"
    // start = input.toString().indexOf('{');
    // end = input.toString().lastIndexOf('}');
    // if (start > -1 && end > -1 && end > start) {
    // byte[] foo = input.toString().substring(start, end).getBytes();
    // byte[] bar = new byte[foo.length];
    // int j = 0;
    // boolean inquote = false;
    // int length = foo.length;
    // for (int i = 0; i < foo.length; i++) {
    // if (foo[i] == (byte) '\"') {
    // inquote = !inquote;
    // continue;
    // }
    // if (inquote) {
    // bar[j++] = foo[i];
    // } else if (foo[i] != (byte) '\n' && foo[i] != (byte) '\t'
    // && foo[i] != (byte) ' ') {
    // bar[j++] = foo[i];
    // }
    // length = j - 1;
    // }
    // start = 0;
    // String name = getName(new String(bar, start, length - 1));
    // retval = new RecordCapsule(name, null);
    // start = start + name.length() + 2;
    // String temp = new String(bar, start + 1, length - start - 1)
    // .replace("},", "|");
    // StringTokenizer stok = new StringTokenizer(temp, "|");
    // while (stok.hasMoreTokens()) {
    // String token = stok.nextToken().replace(":{", "|");
    // retval.addDataCapsule(getDataCapsule(token), false);
    // }
    // }
    //
    // }
    // return retval;
    // }
    //
    // private static DataCapsule getDataCapsule(String string) {
    // // TODO Auto-generated method stub
    // DataCapsule retval = null;
    // StringTokenizer stok1 = new StringTokenizer(string, "|");
    // String name = stok1.nextToken();
    // String temp = stok1.nextToken();
    // retval = new DataCapsule(name, null);
    //
    // HashMap<String,String> hashtemp = new HashMap<String,String>();
    // if (!temp.contains("[")) {
    // StringTokenizer stok2 = new StringTokenizer(temp, ",");
    // while (stok2.hasMoreTokens()) {
    // String temp2 = stok2.nextToken();
    // StringTokenizer stok3 = new StringTokenizer(temp2, ":");
    // hashtemp.put(stok3.nextToken(), stok3.nextToken());
    // }
    // if (hashtemp.containsKey("value")) {
    // if (hashtemp
    // .containsKey(AleppoConstants.ALEPPO_DC_MDATA_TYPE_KEY)) {
    //
    // String c = (String) hashtemp
    // .get(AleppoConstants.ALEPPO_DC_MDATA_TYPE_KEY);
    // if( c.equals("java.lang.Integer")){
    // retval.setData(Integer.parseInt((String) hashtemp.get("value")));
    // }else if( c.equals("java.lang.Float")){
    // retval.setData(Float.parseFloat((String) hashtemp.get("value")));
    // }else if( c.equals("java.lang.Double")){
    // retval.setData(Double.parseDouble((String) hashtemp.get("value")));
    // }else if( c.equals("java.lang.Boolean")){
    // retval.setData(Boolean.parseBoolean((String) hashtemp.get("value")));
    // }else{
    // retval.setData(hashtemp.get("value").toString());
    //
    // }
    //
    // }
    // }
    // } else {
    //
    // int s = string.indexOf('[');
    // int e = string.indexOf(']');
    // StringTokenizer stok2 = new StringTokenizer(string.substring(s+1,e),",");
    // String[] ar = new String[stok2.countTokens()];
    // int i = 0;
    // while( stok2.hasMoreTokens()){
    // ar[i++] = stok2.nextToken();
    // }
    // retval.setData(ar);
    // }
    // return retval;
    // }
    //
    // private static String getName(String buff) {
    // int start = buff.indexOf("{");
    // int end = buff.indexOf(":");
    // return buff.substring(start + 1, end);
    // }
    //
    // public void setProperties(Properties props) {
    // // TODO Auto-generated method stub
    //
    // }
    //
    // private int start = 0;
    // private int end = 0;

}
