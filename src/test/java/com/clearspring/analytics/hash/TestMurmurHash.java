package com.clearspring.analytics.hash;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author epollan
 */
public class TestMurmurHash {

    @Test
    public void testHashByteArrayOverload() {
        String input = "hashthis";
        byte[] inputBytes = input.getBytes();

        int hashOfString = MurmurHash.hash(input);
        assertEquals("MurmurHash.hash(byte[]) did not match MurmurHash.hash(String)",
                     hashOfString, MurmurHash.hash(inputBytes));

        Object bytesAsObject = inputBytes;
        assertEquals("MurmurHash.hash(Object) given a byte[] did not match MurmurHash.hash(String)",
                     hashOfString, MurmurHash.hash(bytesAsObject));
    }

    @Test
    public void testHash64ByteArrayOverload() {
        String input = "hashthis";
        byte[] inputBytes = input.getBytes();

        long hashOfString = MurmurHash.hash64(input);
        assertEquals("MurmurHash.hash64(byte[]) did not match MurmurHash.hash64(String)",
                     hashOfString, MurmurHash.hash64(inputBytes));

        Object bytesAsObject = inputBytes;
        assertEquals("MurmurHash.hash64(Object) given a byte[] did not match MurmurHash.hash64(String)",
                     hashOfString, MurmurHash.hash64(bytesAsObject));
    }

    // test the returned valued of hash functions against the reference implementation: https://github.com/aappleby/smhasher.git

    @Test
    public void testHash64() throws Exception {
        final long actualHash = MurmurHash.hash64("hashthis");
        final long expectedHash = -8896273065425798843L;

        assertEquals("MurmurHash.hash64(String) returns wrong hash value", expectedHash, actualHash);
    }

    @Test
    public void testHash() throws Exception {
        final long actualHash = MurmurHash.hash("hashthis");
        final long expectedHash = -1974946086L;

        assertEquals("MurmurHash.hash(String) returns wrong hash value", expectedHash, actualHash);
    }
}