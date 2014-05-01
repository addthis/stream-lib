package com.clearspring.analytics;

import java.io.*;

public class TestUtils {

    public static byte[] serialize(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        ObjectOutputStream out = null;
        try {
            // stream closed in the finally
            out = new ObjectOutputStream(baos);
            out.writeObject(obj);
        } finally {
            if (out != null) {
                out.close();
            }
        }
        return baos.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws ClassNotFoundException, IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream in = null;
        try {
            // stream closed in the finally
            in = new ObjectInputStream(bais);
            return in.readObject();
        } finally {
            if (in != null) {
                in.close();
            }
        }

    }
}
