package com.custom.stream.operation.intermediate;

import com.custom.stream.exception.ShortCircuitException;
import com.custom.stream.pipeline.DownstreamSource;

/**
 * The source operation at the head of the pipeline.
 *
 * <p>When a terminal operation is called, this operation iterates the source array
 * and pushes each element downstream through the chain of stages built up by
 * intermediate operations. Catches {@link ShortCircuitException} to support
 * early termination by stateful operations such as {@code limit}.
 *
 * @param <T> the element type
 */
public record HeadOperation<T>(T[] source) implements IntermediateOperation<T> {

    @Override
    public void operate(DownstreamSource<T> downstream) {
        try {
            for (T element : source) {
                downstream.process(element);
            }
        } catch (ShortCircuitException ignored) {
            // A stateful operation (e.g. limit) signalled early termination
        }
    }
}
