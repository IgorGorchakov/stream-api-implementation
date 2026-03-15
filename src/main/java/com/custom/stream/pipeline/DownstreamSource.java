package com.custom.stream.pipeline;


/**
 * Functional interface representing a downstream consumer that receives
 * a single element at a time during pipeline evaluation.
 *
 * @param <T> the element type
 */
@FunctionalInterface
public interface DownstreamSource<T> {

    void process(T element);
}
