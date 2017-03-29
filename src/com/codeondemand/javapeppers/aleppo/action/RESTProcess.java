/**
 *
 */
package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.AleppoMessages;
import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * The RESTProcess component invokes a URL with the expectation that the
 * endpoint of that URL is a REST type of service that returns some type of
 * information. The result returned form the service will be placed in a
 * DataCapsule with the name REST_RESPONSE. When initializing this component,
 * the TreeMap passed should have a element keyed with RESTProcess.URL_STRING
 * which specifies the string.
 * <p>
 * It is expected that if dynamic tokens are required in the string specifying
 * the url (including the payload) the tokens will consist of the name of the
 * DataCapsule in the data stream, enclosed with the '%' character. For
 * instance, if there is a token that needs to be replaced with the value of a
 * DataCapsule named 'CITY', the url string will contain %CITY% so it can be
 * substituted with the value carried in that data capsule.
 * <p>
 * Note: If the values in the DataCapsule causes the url string to contain
 * spaces, they will be replaced by a '+' character before the url is invoked.
 *
 * @author gfa
 */
public class RESTProcess extends RecordProcessor {

    public boolean doInitialization(RecordCapsule input) {
        boolean retval = false;

        if (pmap.containsKey(URL_STRING) && pmap.get(URL_STRING) instanceof String) {
            urlstring = (String) pmap.get(URL_STRING);
            retval = true;
        } else {
            logger.error(AleppoMessages.getString("RESTProcess.0")); //$NON-NLS-1$
        }
        if (pmap.containsKey("username")) {
            username = (String) pmap.get("username");
        }
        if (pmap.containsKey("password")) {
            password = (String) pmap.get("password");
        }
        url = createURL(input);
        return retval;
    }

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {
        if (!initialized) {
            initialized = doInitialization(input);
        }
        return processResponse(input, doRESTRequest(input));
    }

    protected URL createURL(RecordCapsule input) {
        URL retval = null;
        if (urlstring != null) {
            String temp = input.replaceTokens(urlstring);
            temp = temp.replaceAll("\\s+", "+"); //$NON-NLS-1$ //$NON-NLS-2$
            try {
                retval = new URL(temp);
            } catch (IOException e) {
                logger.error(e.toString());
            }
        }
        return retval;
    }

    protected String doRESTRequest(RecordCapsule input) {
        String retval = null;
        try {
            urlconn = url.openConnection();
            String basicauth = "Basic " + new String(Base64.encodeBase64(new String(username + ":" + password).getBytes()));
            urlconn.setRequestProperty("Authorization", basicauth);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(urlconn.getInputStream()))) {
                String str = null;
                StringBuilder result = new StringBuilder();
                while ((str = in.readLine()) != null) {
                    result.append(str + " "); //$NON-NLS-1$
                }
                retval = result.toString();
                in.close();
            }
        } catch (IOException e) {
            logger.error(e.toString());
        }

        return retval;
    }

    protected static RecordCapsule processResponse(RecordCapsule input, String response) {
        if (response != null && response.length() > 0) {
            input.addDataCapsule(new DataCapsule(REST_RESPONSE, response.toString()), false);
        } else {
            input.addDataCapsule(new DataCapsule(REST_RESPONSE, AleppoMessages.getString("RESTProcess.5")), false); //$NON-NLS-1$
        }
        return input;
    }

    public final static String REST_RESPONSE = "REST_RESPONSE"; //$NON-NLS-1$
    public final static String URL_STRING = "url"; //$NON-NLS-1$
    protected String username = null;
    protected String password = null;
    protected URL url = null;
    protected boolean initialized = false;

    protected String urlstring = null;
    protected URLConnection urlconn = null;

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("RESTProcess");

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean doInitialization() {
        // TODO Auto-generated method stub
        return true;
    }

}
