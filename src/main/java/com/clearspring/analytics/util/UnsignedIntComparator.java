package com.clearspring.analytics.util;


import java.util.Comparator;

public class UnsignedIntComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] left, byte[] right) {
        int l = Varint.readUnsignedVarInt(left);
        int r = Varint.readUnsignedVarInt(right);
        return l - r;
    }
}