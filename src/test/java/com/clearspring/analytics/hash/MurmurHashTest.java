package com.clearspring.analytics.hash;

import com.clearspring.analytics.hash.MurmurHash;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class MurmurHashTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void hash64Input0NegativeOutputPositive() {

    // Arrange
    final byte[] data = {};
    final int length = -1_825_536_057;

    // Act
    final long retval = MurmurHash.hash64(data, length);

    // Assert result
    Assert.assertEquals(532_139_613_051_938_052L, retval);
  }

  @Test
  public void hash64Input0ZeroZeroOutputZero() {

    // Arrange
    final byte[] data = {};
    final int length = 0;
    final int seed = 0;

    // Act
    final long retval = MurmurHash.hash64(data, length, seed);

    // Assert result
    Assert.assertEquals(0L, retval);
  }

  @Test
  public void hash64Input1PositivePositiveOutputPositive() {

    // Arrange
    final byte[] data = {(byte)0};
    final int length = 1;
    final int seed = 1_516_308_488;

    // Act
    final long retval = MurmurHash.hash64(data, length, seed);

    // Assert result
    Assert.assertEquals(8_112_217_426_270_966_022L, retval);
  }

  @Test
  public void hash64InputNullPositiveNegativeOutputNullPointerException() {

    // Arrange
    final byte[] data = null;
    final int length = 41_595_974;
    final int seed = -934_718_437;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hash64InputNullPositiveNegativeOutputNullPointerException2() {

    // Arrange
    final byte[] data = null;
    final int length = 7;
    final int seed = -1_634_048_354;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hash64InputNullPositivePositiveOutputNullPointerException() {

    // Arrange
    final byte[] data = null;
    final int length = 2;
    final int seed = 1_496_779_358;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hash64InputNullPositivePositiveOutputNullPointerException2() {

    // Arrange
    final byte[] data = null;
    final int length = 5;
    final int seed = 625_996_457;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hash64InputNullPositivePositiveOutputNullPointerException3() {

    // Arrange
    final byte[] data = null;
    final int length = 6;
    final int seed = 1_220_346_942;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hash64InputNullPositivePositiveOutputNullPointerException4() {

    // Arrange
    final byte[] data = null;
    final int length = 4;
    final int seed = 2_129_536_234;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hash64InputNullPositivePositiveOutputNullPointerException5() {

    // Arrange
    final byte[] data = null;
    final int length = 3;
    final int seed = 769_686_320;

    // Act
    thrown.expect(NullPointerException.class);
    MurmurHash.hash64(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hashInput0OutputNegative() {

    // Arrange
    final byte[] data = {};

    // Act
    final int retval = MurmurHash.hash(data);

    // Assert result
    Assert.assertEquals(-1_285_986_640, retval);
  }

  @Test
  public void hashInput0PositivePositiveOutputArrayIndexOutOfBoundsException() {

    // Arrange
    final byte[] data = {};
    final int length = 536_870_912;
    final int seed = 536_870_912;

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    MurmurHash.hash(data, length, seed);

    // Method is not expected to return due to exception thrown
  }


  @Test
  public void hashInput0ZeroOutputZero() {

    // Arrange
    final byte[] data = {};
    final int seed = 0;

    // Act
    final int retval = MurmurHash.hash(data, seed);

    // Assert result
    Assert.assertEquals(0, retval);
  }

  @Test
  public void hashInput0ZeroZeroOutputZero() {

    // Arrange
    final byte[] data = {};
    final int length = 0;
    final int seed = 0;

    // Act
    final int retval = MurmurHash.hash(data, length, seed);

    // Assert result
    Assert.assertEquals(0, retval);
  }

  @Test
  public void hashInput1NegativeNegativeOutputArrayIndexOutOfBoundsException() {

    // Arrange
    final byte[] data = {(byte)0};
    final int length = -2_080_374_782;
    final int seed = -2_080_374_782;

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    MurmurHash.hash(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hashInput1PositivePositiveOutputArrayIndexOutOfBoundsException() {

    // Arrange
    final byte[] data = {(byte)0};
    final int length = 3;
    final int seed = 3;

    // Act
    thrown.expect(ArrayIndexOutOfBoundsException.class);
    MurmurHash.hash(data, length, seed);

    // Method is not expected to return due to exception thrown
  }

  @Test
  public void hashInput1PositivePositiveOutputZero() {

    // Arrange
    final byte[] data = {(byte)0};
    final int length = 1;
    final int seed = 1;

    // Act
    final int retval = MurmurHash.hash(data, length, seed);

    // Assert result
    Assert.assertEquals(0, retval);
  }

  @Test
  public void hashLongInputZeroOutputZero() {

    // Arrange
    final long data = 0L;

    // Act
    final int retval = MurmurHash.hashLong(data);

    // Assert result
    Assert.assertEquals(0, retval);
  }
}
