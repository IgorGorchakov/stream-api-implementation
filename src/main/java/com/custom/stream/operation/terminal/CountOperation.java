package com.custom.stream.operation.terminal;

import com.custom.stream.pipeline.UpstreamSource;

/**
 * Terminal operation that counts the number of elements in the pipeline.
 *
 * <p>Triggers pipeline evaluation and increments an internal counter for
 * each element received.
 *
 * @param <T> the element type
 * @param <R> unused type parameter (result is always {@link Long})
 */
public class CountOperation<T, R> implements TerminalOperation<T, Long> {

    @Override
    public Long operate(UpstreamSource<T> upstreamSource) {
        long[] counter = {0};
        upstreamSource.pushToDownstream(element -> counter[0]++);
        return counter[0];
    }
}
