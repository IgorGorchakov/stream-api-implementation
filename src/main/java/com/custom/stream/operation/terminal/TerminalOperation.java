package com.custom.stream.operation.terminal;

import com.custom.stream.pipeline.UpstreamSource;


/**
 * Represents an eager terminal operation that triggers pipeline evaluation
 * and produces a result.
 *
 * @param <T> the element type consumed from the pipeline
 * @param <R> the result type produced by this operation
 */
public interface TerminalOperation<T, R> {

    R operate(UpstreamSource<T> upstreamSource);
}
