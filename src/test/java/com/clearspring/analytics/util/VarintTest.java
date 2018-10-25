// Copyright (c) Diffblue Limited. All rights reserved. 
// Licensed under the Apache 2.0 license.

package com.clearspring.analytics.util;

import com.clearspring.analytics.util.Varint;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class VarintTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void readSignedVarIntInput0OutputPositive() {

    // Arrange
    final byte[] bytes = {};

    // Act
    final int retval = Varint.readSignedVarInt(bytes);

    // Assert result
    Assert.assertEquals(2_147_483_584, retval);
  }

  @Test
  public void readUnsignedVarIntInput0OutputNegative() {

    // Arrange
    final byte[] bytes = {};

    // Act
    final int retval = Varint.readUnsignedVarInt(bytes);

    // Assert result
    Assert.assertEquals(-128, retval);
  }

  @Test
  public void readUnsignedVarIntInput1OutputNegative() {

    // Arrange
    final byte[] bytes = {(byte)-128};

    // Act
    final int retval = Varint.readUnsignedVarInt(bytes);

    // Assert result
    Assert.assertEquals(-16_384, retval);
  }

  @Test
  public void readUnsignedVarIntInput1OutputZero() {

    // Arrange
    final byte[] bytes = {(byte)0};

    // Act
    final int retval = Varint.readUnsignedVarInt(bytes);

    // Assert result
    Assert.assertEquals(0, retval);
  }

  @Test
  public void writeSignedVarIntInputPositiveOutput1() {

    // Arrange
    final int value = 1;

    // Act
    final byte[] retval = Varint.writeSignedVarInt(value);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)2}, retval);
  }

  @Test
  public void writeUnsignedVarIntInputPositiveOutput1() {

    // Arrange
    final int value = 1;

    // Act
    final byte[] retval = Varint.writeUnsignedVarInt(value);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)1}, retval);
  }
}
