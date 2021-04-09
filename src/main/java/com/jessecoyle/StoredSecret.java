package com.jessecoyle;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.Base64;
import java.util.Map;

import static java.lang.System.*;

/**
 * Represents a row in a credstash table.
 * The encrypted key and encrypted contents are both stored base64 encoded.
 * The hmac digest is stored hex encoded.
 */
class StoredSecret {
    private byte[] key;
    private byte[] contents;
    private byte[] hmac;

    private static byte[] base64AttributeValueToBytes(AttributeValue value) {
        out.println("StoredSecret.base64AttributeValueToBytes(" + value.toString() + ")");
        return Base64.getDecoder().decode(value.getS());
    }

    private static byte[] hexAttributeValueToBytes(AttributeValue value) {
        try {
            out.println("StoredSecret.hexAttributeValueToBytes(" + value.toString() + ")");
            out.println("StoredSecret.hexAttributeValueToBytes: value.getS(): " + value.getS());
            return Hex.decodeHex(value.getS().toCharArray());
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    StoredSecret(Map<String, AttributeValue> item) {
        this.key = base64AttributeValueToBytes(item.get("key"));
        this.contents = base64AttributeValueToBytes(item.get("contents"));
        this.hmac = hexAttributeValueToBytes(item.get("hmac"));
    }

    byte[] getKey() {
        return key;
    }

    byte[] getContents() {
        return contents;
    }

    byte[] getHmac() {
        return hmac;
    }
}
