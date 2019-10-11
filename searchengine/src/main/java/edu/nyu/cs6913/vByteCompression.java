package edu.nyu.cs6913;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class vByteCompression {
    private static final int VAR_BYTE_NUM = 128;
    public vByteCompression() {
    }
    public byte[] vByteEncode(long docID) {
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

    public long vByteDecode(byte[] encoded) {
        final int len = encoded.length;
        long decoded = 0;
        for (int i = 0; i < len; i++) {
            long value = encoded[i] & 0x7F;
            decoded += value * Math.pow(VAR_BYTE_NUM, i);
        }
        return decoded;
    }

    public static void main(String[] args) {
        vByteCompression test = new vByteCompression();
        System.out.println(test.vByteDecode(test.vByteEncode(162)));
    }
}
