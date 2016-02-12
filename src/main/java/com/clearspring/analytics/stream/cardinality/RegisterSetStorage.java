package com.clearspring.analytics.stream.cardinality;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

public interface RegisterSetStorage {
    int get(int bucket);
    void set(int bucket, int value);
    int size();
    int[] readOnlyBits();
    Type type();

    enum Type {
        ARRAY_BACKED(1),
        OFFHEAP(2);
        public int type;

        Type(int type) {
            this.type = type;
        }

        public static Type fromInt(int storageType) {
            for (Type t : values())
                if (t.type == storageType)
                    return t;
            throw new RuntimeException("Unknown storage type: "+storageType);
        }
    }

    class ArrayBackedRegisterSetStorage implements RegisterSetStorage {
        private final int[] M;

        public ArrayBackedRegisterSetStorage(int size) {
            M = new int[size];
        }

        public ArrayBackedRegisterSetStorage(int [] initialValues) {
            M = initialValues;
        }

        @Override
        public int get(int bucket) {
            return M[bucket];
        }

        @Override
        public void set(int bucket, int value) {
            M[bucket] = value;
        }

        @Override
        public int size() {
            return M.length;
        }

        @Override
        public int[] readOnlyBits() {
            return M;
        }

        @Override
        public Type type() {
            return Type.ARRAY_BACKED;
        }
    }

    class OffHeapRegisterSetStorage implements RegisterSetStorage {
        private final Unsafe unsafe;
        private long peer;
        private final int size;

        public OffHeapRegisterSetStorage(int size) {
            try {
                Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = (sun.misc.Unsafe) field.get(null);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            peer = unsafe.allocateMemory(size * 4);
            for (int i = 0; i < size; i++)
                set(i, 0);
            this.size = size;
        }

        public OffHeapRegisterSetStorage(int [] initialValues) {
            this(initialValues.length);
            for (int i = 0; i < initialValues.length; i++)
                set(i, initialValues[i]);
        }

        @Override
        public int get(int bucket) {
            return unsafe.getInt(peer + bucket * 4);
        }

        @Override
        public void set(int bucket, int value) {
            unsafe.putInt(peer + bucket * 4, value);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int[] readOnlyBits() {
            int [] x = new int[size];
            for (int i = 0; i < size; i++)
                x[i] = get(i);
            return x;
        }

        @Override
        public Type type() {
            return Type.OFFHEAP;
        }

        public void finalize() throws Throwable {
            super.finalize();
            if (peer != 0) {
                unsafe.freeMemory(peer);
                peer = 0;
            }
        }
    }
}