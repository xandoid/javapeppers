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
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

public class EncryptionTransform extends RecordTransform {

    @Override
    public RecordCapsule doTransform(RecordCapsule input) {
        if (!initialized) {
            initialized = doInitialization();
        }
        if (initialized) {
            if (input.getField(input_field) != null && !input.getField(input_field).isNull()) {
                String foo = input.getField(input_field).getData().toString();
                byte[] etxt = encrypt(foo, pkey);
                DataCapsule dc = new DataCapsule(output_field, etxt);
                input.addDataCapsule(dc, false);
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
                pkey = this.getPublicKey((String) pmap.get("key_file"));
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

    protected PublicKey getPublicKey(String filename) {

        PublicKey retval = null;

        File f = new File(filename);
        try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
            byte[] keyBytes = new byte[(int) f.length()];
            dis.readFully(keyBytes);
            dis.close();
            X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            KeyFactory kf;

            kf = KeyFactory.getInstance("RSA");
            retval = kf.generatePublic(spec);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return retval;
    }

    private static byte[] encrypt(String text, PublicKey key) {
        byte[] cipherText = null;
        try {
            // get an RSA cipher object and print the provider
            final Cipher cipher = Cipher.getInstance("RSA");
            // encrypt the plain text using the public key
            cipher.init(Cipher.ENCRYPT_MODE, key);
            cipherText = cipher.doFinal(text.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cipherText;
    }

    protected boolean initialized = false;
    protected String input_field = null;
    protected String output_field = null;
    protected PublicKey pkey = null;

    @Override
    public void done() {
        // TODO Auto-generated method stub

    }

}
