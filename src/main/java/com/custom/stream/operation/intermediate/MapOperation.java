package com.custom.stream.operation.intermediate;

import com.custom.stream.predicate.MapperPredicate;
import com.custom.stream.pipeline.DownstreamSource;
import com.custom.stream.pipeline.UpstreamSource;

/**
 * Intermediate operation that transforms each element using the given mapper.
 *
 * <p>Applies the {@link MapperPredicate#map(Object)} function to every upstream
 * element and forwards the result downstream, potentially changing the element type.
 *
 * @param <IN>  the input element type
 * @param <OUT> the output element type after transformation
 */
public record MapOperation<IN, OUT>(
        UpstreamSource<IN> upstreamSource,
        MapperPredicate<IN, OUT> mapperPredicate
) implements IntermediateOperation<OUT> {

    @Override
    public void operate(DownstreamSource<OUT> downstream) {
        upstreamSource.pushToDownstream(element -> downstream.process(mapperPredicate.map(element)));
    }
}
