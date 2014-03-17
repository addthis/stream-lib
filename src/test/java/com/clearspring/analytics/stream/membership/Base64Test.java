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

package com.clearspring.analytics.stream.membership;

import java.io.IOException;

import java.nio.charset.Charset;

import com.google.common.io.Resources;

import org.apache.commons.codec.binary.Base64;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class Base64Test {

    @Test
    public void testBase64EncodedBloomFilter() throws IOException, ClassNotFoundException {
        BloomFilter bf = BloomFilter.deserialize(Base64.decodeBase64(Resources.toString(Resources.getResource(Base64Test.class, "encoded_random_keys.bloom"), Charset.forName("UTF-8"))));
        assertTrue(bf.isPresent("4a7137513e61adbb"));
        assertTrue(bf.isPresent("4ba145c986af5848"));
        assertTrue(bf.isPresent("4b8c73a241c9d017"));
        assertTrue(bf.isPresent("4bafd549baae6a0c"));
        assertTrue(bf.isPresent("4b98ed851c5fc689"));
        assertTrue(bf.isPresent("4bbead53d3600f7c"));
        assertTrue(bf.isPresent("4bc21f2d4a4a8941"));
        assertTrue(bf.isPresent("4b991b45226abc99"));
        assertFalse(bf.isPresent("blurg"));
        assertFalse(bf.isPresent("bowzer"));
        assertFalse(bf.isPresent("4b991b45226abc90"));
    }
}
