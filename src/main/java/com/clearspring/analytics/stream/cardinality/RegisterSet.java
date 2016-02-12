/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
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

package com.clearspring.analytics.stream.cardinality;

public class RegisterSet {

    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;

    private final RegisterSetStorage M;

    public RegisterSet(int count) {
        this(count, null);
    }

    public RegisterSet(int count, int[] initialValues) {
        this(count, initialValues, RegisterSetStorage.Type.ARRAY_BACKED);
    }

    public RegisterSet(int count, int[] initialValues, RegisterSetStorage.Type type) {
        this.count = count;

        switch (type) {
            case ARRAY_BACKED:
                if (initialValues == null)
                    M = new RegisterSetStorage.ArrayBackedRegisterSetStorage(getSizeForCount(count));
                else
                    M = new RegisterSetStorage.ArrayBackedRegisterSetStorage(initialValues);
                break;
            case OFFHEAP:
                if (initialValues == null)
                    M = new RegisterSetStorage.OffHeapRegisterSetStorage(getSizeForCount(count));
                else
                    M = new RegisterSetStorage.OffHeapRegisterSetStorage(initialValues);
                break;
            default:
                throw new RuntimeException("Unsupported RegisterSetStorage: " + type);
        }
    }

    public static int getBits(int count) {
        return count / LOG2_BITS_PER_WORD;
    }

    public static int getSizeForCount(int count) {
        int bits = getBits(count);
        if (bits == 0) {
            return 1;
        } else if (bits % Integer.SIZE == 0) {
            return bits;
        } else {
            return bits + 1;
        }
    }

    public void set(int position, int value) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        M.set(bucketPos, (M.get(bucketPos) & ~(0x1f << shift)) | (value << shift));
    }

    public int get(int position) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (M.get(bucketPos) & (0x1f << shift)) >>> shift;
    }

    public boolean updateIfGreater(int position, int value) {
        int bucket = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift;

        // Use long to avoid sign issues with the left-most shift
        long curVal = M.get(bucket) & mask;
        long newVal = value << shift;
        if (curVal < newVal) {
            M.set(bucket, (int) ((M.get(bucket) & ~mask) | newVal));
            return true;
        } else {
            return false;
        }
    }

    public void merge(RegisterSet that) {
        for (int bucket = 0; bucket < M.size(); bucket++) {
            int word = 0;
            for (int j = 0; j < LOG2_BITS_PER_WORD; j++) {
                int mask = 0x1f << (REGISTER_SIZE * j);

                int thisVal = this.M.get(bucket) & mask;
                int thatVal = that.M.get(bucket) & mask;
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }
            M.set(bucket, word);
        }
    }

    int[] readOnlyBits() {
        return M.readOnlyBits();
    }

    public int[] bits() {
        int [] bits = M.readOnlyBits();
        int[] copy = new int[bits.length];
        System.arraycopy(bits, 0, copy, 0, bits.length);
        return copy;
    }

    public int size() {
        return M.size();
    }

    public RegisterSetStorage.Type storageType() {
        return M.type();
    }
}
