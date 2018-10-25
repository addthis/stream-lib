// Copyright (c) Diffblue Limited. All rights reserved. 
// Licensed under the Apache 2.0 license.

package com.clearspring.analytics.hash;

import com.clearspring.analytics.hash.Lookup3Hash;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class Lookup3HashTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void lookup3Input0PositiveNegativeNegativeOutputNegative() {

    // Arrange
    final int[] k = {};
    final int offset = 262_145;
    final int length = -2_147_483_643;
    final int initval = -485_474_051;

    // Act
    final int retval = Lookup3Hash.lookup3(k, offset, length, initval);

    // Assert result
    Assert.assertEquals(-1_044_512_768, retval);
  }

  @Test
  public void lookup3Input1ZeroPositiveNegativeOutputArrayIndexOutOfBoundsException() {

    // Arrange
    final int[] k = {-419_772_902};
    final int offset = 0;
    final int length = 11;
    final int initval = -1_553_263_165;

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    Lookup3Hash.lookup3(k, offset, length, initval);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void lookup3Input2PositivePositivePositiveOutputArrayIndexOutOfBoundsException() {

    // Arrange
    final int[] k = {240_498_410, 16_843_272};
    final int offset = 536_870_911;
    final int length = 1;
    final int initval = 992_639_440;

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    Lookup3Hash.lookup3(k, offset, length, initval);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void lookup3Input2ZeroPositiveNegativeOutputArrayIndexOutOfBoundsException() {

    // Arrange
    final int[] k = {-419_772_902, 0};
    final int offset = 0;
    final int length = 11;
    final int initval = -1_553_263_165;

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    Lookup3Hash.lookup3(k, offset, length, initval);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void lookup3InputNullNegativePositivePositiveOutputNullPointerException() {

    // Arrange
    final int[] k = null;
    final int offset = -2_147_483_647;
    final int length = 6;
    final int initval = 6208;

    // Act
    thrown.expect(NullPointerException.class);
    Lookup3Hash.lookup3(k, offset, length, initval);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void lookup3InputNullPositivePositivePositiveOutputNullPointerException() {

    // Arrange
    final int[] k = null;
    final int offset = 2_143_289_337;
    final int length = 3;
    final int initval = 551_731_363;

    // Act
    thrown.expect(NullPointerException.class);
    Lookup3Hash.lookup3(k, offset, length, initval);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void lookup3InputNullPositivePositivePositiveOutputNullPointerException2() {

    // Arrange
    final int[] k = null;
    final int offset = 2_143_289_337;
    final int length = 2;
    final int initval = 552_121_283;

    // Act
    thrown.expect(NullPointerException.class);
    Lookup3Hash.lookup3(k, offset, length, initval);

    // Method is not expected to return due to exception thrown
  }
  
  @Test
  public void lookup3ycsInput0NegativeNegativeNegativeOutputPositive() {

    // Arrange
    final int[] k = {};
    final int offset = -2_147_483_647;
    final int length = -1_610_612_735;
    final int initval = -2_147_483_648;

    // Act
    final int retval = Lookup3Hash.lookup3ycs(k, offset, length, initval);

    // Assert result
    Assert.assertEquals(1_588_444_911, retval);
  }
}
