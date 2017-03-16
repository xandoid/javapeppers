package com.codeondemand.javapeppers.aleppo.transform;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

import javax.crypto.Cipher;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

public class DecryptionTransform extends RecordTransform {

    @Override
    public RecordCapsule doTransform(RecordCapsule input) {
        if (!initialized) {
            initialized = doInitialization();
            if (initialized) {
                if (input.getField(input_field) != null && !input.getField(input_field).isNull()) {
                    byte[] foo = (byte[]) input.getField(input_field).getData();
                    String txt = decrypt(foo, pkey);
                    DataCapsule dc = new DataCapsule(output_field, txt);
                    input.addDataCapsule(dc, false);
                }
            }
        }
        return input;
    }

    @Override
    public boolean doInitialization() {
        boolean retval = true;

        if (pmap.containsKey("input_field")) {
            input_field = (String) pmap.get("input_field");
        }
        if (pmap.containsKey("output_field")) {
            output_field = (String) pmap.get("output_field");
        }

        if (pmap.containsKey("key_file")) {
            try {
                pkey = getPrivateKey((String) pmap.get("key_file"));
                if (pkey == null) {
                    retval = false;
                }
            } catch (Exception e) {
                retval = false;
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return retval;
    }

    private static PrivateKey getPrivateKey(String filename) {
        PrivateKey retval = null;
        try {
            File f = new File(filename);
            byte[] keyBytes = new byte[(int) f.length()];

            try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
                dis.readFully(keyBytes);
                dis.close();
                PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
                KeyFactory kf = KeyFactory.getInstance("RSA");
                retval = kf.generatePrivate(spec);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return retval;
    }

    private static String decrypt(byte[] text, PrivateKey key) {
        String retval = null;
        byte[] decryptedText = null;
        try {
            // get an RSA cipher object and print the provider
            final Cipher cipher = Cipher.getInstance("RSA");

            // decrypt the text using the private key
            cipher.init(Cipher.DECRYPT_MODE, key);
            decryptedText = cipher.doFinal(text);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (decryptedText != null) {
            retval = new String(decryptedText);
        }
        return retval;
    }

    protected boolean initialized = false;
    protected String input_field = null;
    protected String output_field = null;
    protected PrivateKey pkey = null;

    public void done() {
        // TODO Auto-generated method stub

    }

}
