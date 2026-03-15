package com.custom.stream.pipeline;

import com.custom.stream.operation.intermediate.HeadOperation;

/**
 * The source pipeline stage that holds the original data array.
 *
 * <p>Serves as the entry point of every stream created via
 * {@link com.custom.stream.Stream#of(Object[])}. Delegates source iteration
 * to a {@link HeadOperation}.
 *
 * @param <T> the element type
 */
public class HeadPipeline<T> extends OperationalPipeline<T> {

    public HeadPipeline(T[] elements) {
        super(new HeadOperation<>(elements));
    }
}
