/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;


public class Serializer {
    
    public static byte[] toBytes(byte b) throws IOException {
        return new byte[] { b };
    }
    

    public static byte[] toBytes(boolean b) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeBoolean(b);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    public static byte[] toBytes(short i) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeShort(i);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    public static byte[] toBytes(int i) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeInt(i);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    public static byte[] toBytes(long x) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeLong(x);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    public static byte[] toBytes(float f) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeFloat(f);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    public static byte[] toBytes(double d) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeDouble(d);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    public static byte[] toBytes(char c) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream datas = null;
        
        try {
            datas = new DataOutputStream(out);
            datas.writeChar(c);
        } finally {
            if ( datas != null ) datas.close();
        }
        
        try {
            return out.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    

    /**
     * There is no corresponding signature for String as
     * ObjectOutputStream handles it transparently.
     */
    public static byte[] toBytes(Object o) throws IOException {
        byte[] bytes = null;
        ObjectOutput out = null;
        ByteArrayOutputStream baos = null;
        
        try {
            baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(o);
        } finally {
            if ( out != null ) out.close();
        }
        
        try {
            bytes = baos.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
        
        return bytes;
    }
    
    
    /**
     * Attempts to determine if supplied object is a boxed raw type, and appropriately 
     * serializes it if so; otherwise serializes it as an object.
     * 
     * @param o Object or raw type to be serialized.
     * @return Serialized bytes.
     * @throws IOException
     */
    public static byte[] toRawBytes(Object o) throws IOException {
        if (o instanceof Byte)
            return toBytes(((Byte) o).byteValue());
        if (o instanceof Boolean)
            return toBytes(((Boolean) o).booleanValue());
        if (o instanceof Short)
            return toBytes(((Short) o).shortValue());
        if (o instanceof Integer)
            return toBytes(((Integer) o).intValue());
        if (o instanceof Long)
            return toBytes(((Long) o).longValue());
        if (o instanceof Float)
            return toBytes(((Float) o).floatValue());
        if (o instanceof Double)
            return toBytes(((Double) o).doubleValue());
        if (o instanceof Character)
            return toBytes(((Character) o).charValue());
        return toBytes(o);
    }
    
    

    public static boolean toBoolean(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readBoolean();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static short toShort(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readShort();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static int toInteger(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readInt();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static long toLong(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readLong();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static float toFloat(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readFloat();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static double toDouble(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readDouble();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static char toChar(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datas = new DataInputStream(in);
        
        try {
            return datas.readChar();
        } finally {
            if ( datas != null ) datas.close();
        }
    }
    

    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Object o = null;
        ObjectInput in = null;
        ByteArrayInputStream bais = null;
        
        try {
            bais = new ByteArrayInputStream(bytes);
            in = new ObjectInputStream(bais);
            o = in.readObject();
        } finally {
            if ( in != null ) in.close();
        }
        
        return o;
    }
    
}
