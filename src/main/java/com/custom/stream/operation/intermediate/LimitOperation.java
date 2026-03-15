package com.custom.stream.operation.intermediate;

import com.custom.stream.pipeline.DownstreamSource;
import com.custom.stream.exception.ShortCircuitException;
import com.custom.stream.pipeline.UpstreamSource;

/**
 * Stateful intermediate operation that truncates the stream to at most
 * {@code maxSize} elements.
 *
 * <p>Once the limit is reached, throws a {@link ShortCircuitException} to signal
 * early pipeline termination, which is caught by {@code HeadOperation}.
 *
 * @param <T> the element type
 */
public record LimitOperation<T>(
        UpstreamSource<T> upstreamSource,
        long maxSize
) implements IntermediateOperation<T> {

    @Override
    public void operate(DownstreamSource<T> downstream) {
        long[] count = {0};
        upstreamSource.pushToDownstream(element -> {
            if (count[0] < maxSize) {
                count[0]++;
                downstream.process(element);
            }
            if (count[0] >= maxSize) {
                throw new ShortCircuitException();
            }
        });
    }
}
