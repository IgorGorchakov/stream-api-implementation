package com.custom.stream.operation.intermediate;

import com.custom.stream.predicate.PeekPredicate;
import com.custom.stream.pipeline.DownstreamSource;
import com.custom.stream.pipeline.UpstreamSource;

/**
 * Intermediate operation that performs a side-effect action on each element
 * without modifying it.
 *
 * <p>Invokes the {@link PeekPredicate#peek(Object)} action and then forwards
 * the element downstream unchanged. Useful for debugging and logging.
 *
 * @param <T> the element type
 */
public record PeekOperation<T>(
        UpstreamSource<T> upstreamSource,
        PeekPredicate<T> peekPredicate
) implements IntermediateOperation<T> {

    @Override
    public void operate(DownstreamSource<T> downstream) {
        upstreamSource.pushToDownstream(element -> {
            peekPredicate.peek(element);
            downstream.process(element);
        });
    }
}
