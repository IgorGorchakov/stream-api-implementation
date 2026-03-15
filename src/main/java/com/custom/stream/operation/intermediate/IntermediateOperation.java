package com.custom.stream.operation.intermediate;

import com.custom.stream.pipeline.DownstreamSource;

/**
 * Represents a lazy intermediate operation in the stream pipeline.
 *
 * <p>Each implementation defines how elements are processed and forwarded
 * to the next downstream stage.
 *
 * @param <T> the output element type of this operation
 */
public interface IntermediateOperation<T> {

    void operate(DownstreamSource<T> downstream);
}
