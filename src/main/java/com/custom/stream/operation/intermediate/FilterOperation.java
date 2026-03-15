package com.custom.stream.operation.intermediate;

import com.custom.stream.pipeline.UpstreamSource;
import com.custom.stream.predicate.FilterPredicate;
import com.custom.stream.pipeline.DownstreamSource;

/**
 * Intermediate operation that retains only elements satisfying the given predicate.
 *
 * <p>Elements that pass the {@link FilterPredicate#check(Object)} test are forwarded
 * downstream; others are silently discarded.
 *
 * @param <T> the element type
 */
public record FilterOperation<T>(
        UpstreamSource<T> upstreamSource,
        FilterPredicate<T> filterPredicate
) implements IntermediateOperation<T> {

    @Override
    public void operate(DownstreamSource<T> downstream) {
        upstreamSource.pushToDownstream(element -> {
            if (filterPredicate.check(element)) {
                downstream.process(element);
            }
        });
    }
}
