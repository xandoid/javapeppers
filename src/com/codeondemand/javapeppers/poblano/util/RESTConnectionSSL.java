package com.codeondemand.javapeppers.poblano.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;
import sun.net.www.protocol.https.HttpsURLConnectionImpl;

import java.io.*;
import java.net.URL;
import java.util.TreeMap;

public class RESTConnectionSSL {

    public RESTConnectionSSL(String urlstring, String uid, String pwd) {
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

    public void setDoOutput() {
        this.doOutput = true;
    }

    public void setPut() {
        this.isPut = true;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setContentType(String content_type) {
        this.content_type = content_type;
    }

    public void setProperty(String name, String value) {
        req_props.put(name, value);
    }

    public String doRESTRequest() {
        String retval = null;

        HttpsURLConnectionImpl urlconn = null;
        BufferedReader in = null;
        try {
            urlconn = (HttpsURLConnectionImpl) url.openConnection();
            if (urlconn != null) {
                urlconn.setReadTimeout(30000);
            }

        } catch (Exception e) {
            // do nothing.
        }

        try {
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
            if (isPut) {
                urlconn.setRequestMethod("PUT");
                if (!req_props.isEmpty()) {
                    for (String s : req_props.keySet()) {
                        String key = s.toString();
                        String val = req_props.get(key);
                        urlconn.setRequestProperty(key, val);
                    }
                }
            }

            String basicauth = "Basic " + new String(Base64.encodeBase64(new String(username + ":" + password).getBytes()));
            if (urlconn != null) {
                urlconn.setRequestProperty("Authorization", basicauth);

                if (doOutput && payload != null) {
                    urlconn.setDoOutput(true);
                    urlconn.setRequestProperty("Content-Type", content_type);
                    urlconn.setRequestProperty("Content-Length", "" + Integer.toString(payload.getBytes().length));

                    OutputStreamWriter wr = new OutputStreamWriter(urlconn.getOutputStream());
                    wr.write(payload);
                    wr.flush();
                    wr.close();
                    System.out.println("Response code:" + urlconn.getResponseCode());
                }


                in = new BufferedReader(new InputStreamReader(urlconn.getInputStream()));
                String str = null;
                StringBuilder result = new StringBuilder();
                while (in.ready() && (str = in.readLine()) != null) {
                    result.append(str + " "); //$NON-NLS-1$
                }
                retval = result.toString();
                in.close();
                urlconn.disconnect();

            }
        } catch (Exception e) {
            logger.error(e.toString() + "|" + request_string);

            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
            urlconn.disconnect();
        }

        return retval;
    }

    public String doRESTRequestBig() {
        String retval = null;
        InputStream istream = null;
        HttpsURLConnectionImpl urlconn = null;
        try {
            urlconn = (HttpsURLConnectionImpl) url.openConnection();

        } catch (Exception e) {
            // do nothing.
        }

        try {
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
            if (isPut) {
                urlconn.setRequestMethod("PUT");
                if (!req_props.isEmpty()) {
                    for (String s : req_props.keySet()) {
                        String key = s.toString();
                        String val = req_props.get(key);
                        urlconn.setRequestProperty(key, val);
                    }
                }
            }

            String basicauth = "Basic " + new String(Base64.encodeBase64(new String(username + ":" + password).getBytes()));

            if (urlconn != null) {
                urlconn.setRequestProperty("Authorization", basicauth);

                if (doOutput && payload != null) {
                    urlconn.setDoOutput(true);
                    urlconn.setRequestProperty("Content-Type", content_type);
                    urlconn.setRequestProperty("Content-Length", "" + Integer.toString(payload.getBytes().length));

                    OutputStreamWriter wr = new OutputStreamWriter(urlconn.getOutputStream());
                    wr.write(payload);
                    wr.flush();
                    wr.close();
                    System.out.println("Response code:" + urlconn.getResponseCode());
                }

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

            }
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
    private boolean isPut = false;
    private boolean doOutput = false;
    private String content_type = null;
    private String payload = null;
    private String request_string = null;
    private TreeMap<String, String> req_props = new TreeMap<>();
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RESTConnectionSSL");

}
