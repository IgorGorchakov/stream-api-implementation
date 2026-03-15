package com.custom.stream.pipeline;


/**
 * Functional interface representing an upstream source that pushes its
 * elements to a {@link DownstreamSource} for processing.
 *
 * <p>Each pipeline stage exposes an {@code UpstreamSource} so that the next
 * stage can trigger upstream iteration.
 *
 * @param <T> the element type
 */
@FunctionalInterface
public interface UpstreamSource<T> {
    void pushToDownstream(DownstreamSource<T> downstream);
}
