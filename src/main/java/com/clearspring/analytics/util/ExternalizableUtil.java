package com.clearspring.analytics.util;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ExternalizableUtil {

    public static byte[] toBytes(Externalizable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        o.writeExternal(out);
        out.flush();
        return baos.toByteArray();
    }
}
