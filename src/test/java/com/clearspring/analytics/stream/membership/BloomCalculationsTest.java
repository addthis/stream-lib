package com.clearspring.analytics.stream.membership;

import com.clearspring.analytics.stream.membership.BloomCalculations.BloomSpecification;
import com.clearspring.analytics.stream.membership.BloomCalculations;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class BloomCalculationsTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void computeBestKInputPositiveOutputPositive() {

    // Arrange
    final int bucketsPerElement = 18;

    // Act
    final int retval = BloomCalculations.computeBestK(bucketsPerElement);

    // Assert result
    Assert.assertEquals(12, retval);
  }

  @Test
  public void computeBestKInputPositiveOutputPositive2() {

    // Arrange
    final int bucketsPerElement = 50;

    // Act
    final int retval = BloomCalculations.computeBestK(bucketsPerElement);

    // Assert result
    Assert.assertEquals(14, retval);
  }

  @Test
  public void computeBucketsAndKInputNaNOutputNotNull() {

    // Arrange
    final double maxFalsePosProb = Double.NaN;

    // Act
    final BloomSpecification retval = BloomCalculations.computeBucketsAndK(maxFalsePosProb);

    // Assert result
    Assert.assertNotNull(retval);
    Assert.assertEquals(2, retval.bucketsPerElement);
    Assert.assertEquals(1, retval.K);
  }

  @Test
  public void computeBucketsAndKInputNegativeOutputNotNull() {

    // Arrange
    final double maxFalsePosProb = -0.1965;

    // Act
    final BloomSpecification retval = BloomCalculations.computeBucketsAndK(maxFalsePosProb);

    // Assert result
    Assert.assertNotNull(retval);
    Assert.assertEquals(15, retval.bucketsPerElement);
    Assert.assertEquals(8, retval.K);
  }

  @Test
  public void computeBucketsAndKInputPositiveInfinityOutputNotNull() {

    // Arrange
    final double maxFalsePosProb = Double.POSITIVE_INFINITY;

    // Act
    final BloomSpecification retval = BloomCalculations.computeBucketsAndK(maxFalsePosProb);

    // Assert result
    Assert.assertNotNull(retval);
    Assert.assertEquals(1, retval.bucketsPerElement);
    Assert.assertEquals(2, retval.K);
  }

  @Test
  public void computeBucketsAndKInputPositiveOutputNotNull() {

    // Arrange
    final double maxFalsePosProb = 0.237;

    // Act
    final BloomSpecification retval = BloomCalculations.computeBucketsAndK(maxFalsePosProb);

    // Assert result
    Assert.assertNotNull(retval);
    Assert.assertEquals(3, retval.bucketsPerElement);
    Assert.assertEquals(2, retval.K);
  }

  @Test
  public void computeBucketsAndKInputPositiveOutputNotNull2() {

    // Arrange
    final double maxFalsePosProb = 0.349;

    // Act
    final BloomSpecification retval = BloomCalculations.computeBucketsAndK(maxFalsePosProb);

    // Assert result
    Assert.assertNotNull(retval);
    Assert.assertEquals(3, retval.bucketsPerElement);
    Assert.assertEquals(1, retval.K);
  }

  @Test
  public void constructorInputZeroZeroOutputVoid() {

    // Arrange
    final int k = 0;
    final int bucketsPerElement = 0;

    // Act, creating object to test constructor
    BloomSpecification objectUnderTest = new BloomSpecification(k, bucketsPerElement);

    // Method returns void, testing that no exception is thrown
  }

  @Test
  public void constructorOutputVoid() {

    // Act, creating object to test constructor
    BloomCalculations objectUnderTest = new BloomCalculations();

    // Method returns void, testing that no exception is thrown
  }
}
