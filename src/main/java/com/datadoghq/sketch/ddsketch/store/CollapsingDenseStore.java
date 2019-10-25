/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.io.Serializable;

abstract class CollapsingDenseStore extends DenseStore implements Serializable {

    private static final long serialVersionUID = 3392875841024915195L;
    private final int maxNumBins;

    boolean isCollapsed;

    CollapsingDenseStore(int maxNumBins) {
        this.maxNumBins = maxNumBins;
        this.isCollapsed = false;
    }

    CollapsingDenseStore(CollapsingDenseStore store) {
        super(store);
        this.maxNumBins = store.maxNumBins;
        this.isCollapsed = store.isCollapsed;
    }

    @Override
    int getNewLength(int desiredLength) {
        return Math.min(super.getNewLength(desiredLength), maxNumBins);
    }
}
