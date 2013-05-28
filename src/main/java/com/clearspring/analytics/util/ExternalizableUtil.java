package com.clearspring.analytics.util;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ExternalizableUtil
{
    public static byte[] toBytes(Externalizable o)
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            o.writeExternal(out);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream does not throw IOException
            throw new IllegalStateException(e);
        }
    }
}
