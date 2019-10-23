/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link LogarithmicMapping}) by
 * extracting the floor value of the logarithm to the base 2 from the binary representations of floating-point values
 * and linearly interpolating the logarithm in-between.
 */
public class LinearlyInterpolatedMapping implements IndexMapping, Serializable {

    private final double relativeAccuracy;
    private final double multiplier;

    public LinearlyInterpolatedMapping(double relativeAccuracy) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        this.relativeAccuracy = relativeAccuracy;
        this.multiplier = 1 / Math.log((1 + relativeAccuracy) / (1 - relativeAccuracy));
    }

    @Override
    public int index(double value) {
        final long longBits = Double.doubleToRawLongBits(value);
        final double index = multiplier * (
            (double) DoubleBitOperationHelper.getExponent(longBits)
                + DoubleBitOperationHelper.getSignificandPlusOne(longBits)
        );
        return index >= 0 ? (int) index : (int) index - 1;
    }

    @Override
    public double value(int index) {
        final double normalizedIndex = (double) index / multiplier;
        final long exponent = (long) Math.floor(normalizedIndex - 1);
        final double significandPlusOne = normalizedIndex - exponent;
        return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne) * (1 + relativeAccuracy);
    }

    @Override
    public double relativeAccuracy() {
        return relativeAccuracy;
    }

    @Override
    public double minIndexableValue() {
        return Math.max(
            Math.pow(2, (Integer.MIN_VALUE + 1) / multiplier), // so that index >= Integer.MIN_VALUE
            Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy)
        );
    }

    @Override
    public double maxIndexableValue() {
        return Math.min(
            Math.pow(2, Integer.MAX_VALUE / multiplier - 1), // so that index <= Integer.MAX_VALUE
            Double.MAX_VALUE / (1 + relativeAccuracy)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return Double.compare(relativeAccuracy, ((LinearlyInterpolatedMapping) o).relativeAccuracy) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativeAccuracy);
    }
}
