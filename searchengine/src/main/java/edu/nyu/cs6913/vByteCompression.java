package edu.nyu.cs6913;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class vByteCompression {
    private static final int VAR_BYTE_NUM = 128;
    vByteCompression() {
    }

    /**
     * takes in a long and output an var-byte encoded byte array
     * @param docID a long of docID
     * @return a var-byte encoded byte array
     */
    byte[] vByteEncode(long docID) {
        List<Byte> byteList = new ArrayList<Byte>();
        while (true) {
            byte temp = (byte) ((docID % VAR_BYTE_NUM) | 0x80);
            byteList.add(temp);
            if (docID < VAR_BYTE_NUM) {
                break;
            }
            docID /= VAR_BYTE_NUM;
        }
        byteList.set(0, (byte) (byteList.get(0) & 0x7F));
        int len = byteList.size();
        byte[] encoded = new byte[len];
        for (int i = 0; i < len; i++) {
            encoded[i] = byteList.get(i);
        }
        return encoded;
    }

    /**
     * decodes var-byte array to long
     * @param encoded a var-byte array
     * @return long
     */
    private long vByteDecode(byte[] encoded) {
        final int len = encoded.length;
        long decoded = 0;
        for (int i = 0; i < len; i++) {
            long value = encoded[i] & 0x7F;
            decoded += value * Math.pow(VAR_BYTE_NUM, i);
        }
        return decoded;
    }

    List<Long> vByteDecodeArray(byte[] encoded) {
        int startIndex = 0;
        final int len = encoded.length;
        ArrayList<Long> decodedList = new ArrayList<>();
        for (int i = 1; i < len; i++) {
            if (encoded[i] >= 0) {
                long decoded = vByteDecode(Arrays.copyOfRange(encoded, startIndex, i));
                decodedList.add(decoded);
                startIndex = i;

            }
        }
        decodedList.add(vByteDecode(Arrays.copyOfRange(encoded, startIndex, len)));
        return decodedList;
    }

    public static void main(String[] args) {
        vByteCompression test = new vByteCompression();
        System.out.println(test.vByteDecode(test.vByteEncode(0)));
        System.out.println(Arrays.toString(test.vByteEncode(0)));
    }
}
