package com.codeondemand.javapeppers.poblano.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.TreeMap;

public class RESTConnection {

    public RESTConnection(String urlstring, String uid, String pwd) {
        this.username = uid;
        this.password = pwd;
        this.request_string = urlstring;
        intializeURL(urlstring);
    }

    private boolean intializeURL(String urlstring) {
        boolean retval = false;
        if (urlstring != null) {
            String temp = urlstring.replaceAll("\\s+", "+"); //$NON-NLS-1$ //$NON-NLS-2$
            try {
                url = new URL(temp);
                retval = true;
            } catch (IOException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    public void setPost() {
        this.isPost = true;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setProperty(String name, String value) {
        req_props.put(name, value);
    }

    public String doRESTRequest() {
        String retval = null;
        HttpURLConnection urlconn = null;
        BufferedReader in = null;
        try {

            urlconn = (HttpURLConnection) url.openConnection();
            if (urlconn != null) {
                urlconn.setReadTimeout(30000);
            }
            if (isPost) {
                urlconn.setRequestMethod("POST");
                if (!req_props.isEmpty()) {
                    for (String s : req_props.keySet()) {
                        String key = s.toString();
                        String val = req_props.get(key);
                        urlconn.setRequestProperty(key, val);
                    }
                }

            }

            String basicauth = "Basic " + new String(Base64.encodeBase64(new String(username + ":" + password).getBytes()));
            urlconn.setRequestProperty("Authorization", basicauth);

            in = new BufferedReader(new InputStreamReader(urlconn.getInputStream()));

            String str = null;
            StringBuilder result = new StringBuilder();
            while (in.ready() && (str = in.readLine()) != null) {
                result.append(str + " "); //$NON-NLS-1$
            }
            retval = result.toString();
            in.close();
            urlconn.disconnect();

        } catch (IOException e) {
            logger.error(e.toString());
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
            if (urlconn != null) {
                urlconn.disconnect();
            }
        }

        return retval;
    }

    public String doRESTRequestBig() {
        String retval = null;
        InputStream istream = null;
        HttpURLConnection urlconn = null;
        try {

            urlconn = (HttpURLConnection) url.openConnection();
            if (isPost) {
                urlconn.setRequestMethod("POST");
                if (!req_props.isEmpty()) {
                    for (String s : req_props.keySet()) {
                        String key = s.toString();
                        String val = req_props.get(key);
                        urlconn.setRequestProperty(key, val);
                    }
                }

            }

            String basicauth = "Basic " + new String(Base64.encodeBase64(new String(username + ":" + password).getBytes()));
            urlconn.setRequestProperty("Authorization", basicauth);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int resp_code = urlconn.getResponseCode();
            if (200 <= resp_code && resp_code <= 299) {
                istream = urlconn.getInputStream();
            } else {
                istream = urlconn.getErrorStream();
            }

            int len = -1;
            byte[] foo = new byte[8196];

            while (-1 != (len = istream.read(foo))) {
                baos.write(foo, 0, len);
            }
            retval = baos.toString();
            istream.close();
            urlconn.disconnect();

        } catch (IOException e) {
            try {
                if (istream != null) {
                    istream.close();
                }
                if (urlconn != null) {
                    urlconn.disconnect();
                }
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            logger.error(e.toString() + "|" + request_string);
        }

        return retval;
    }

    private String username = null;
    private String password = null;
    private URL url = null;
    private boolean isPost = false;
    private String payload = null;
    private String request_string = null;
    private TreeMap<String, String> req_props = new TreeMap<>();
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RESTConnection");

}
