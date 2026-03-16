package com.custom.stream.pipeline;

import com.custom.stream.Stream;
import com.custom.stream.operation.intermediate.*;
import com.custom.stream.operation.terminal.CountOperation;
import com.custom.stream.operation.terminal.ToListOperation;
import com.custom.stream.operation.terminal.TerminalOperation;
import com.custom.stream.predicate.FilterPredicate;
import com.custom.stream.predicate.MapperPredicate;
import com.custom.stream.predicate.PeekPredicate;

import java.util.List;


/**
 * Base pipeline implementation of {@link Stream}.
 *
 * <p>Each {@code OperationalPipeline} holds a reference to its
 * {@link IntermediateOperation} and provides concrete implementations of all
 * intermediate and terminal stream operations. Every intermediate call creates
 * a new {@code OperationalPipeline} that wraps a new operation linked to the
 * previous one via an {@link UpstreamSource}; every terminal call triggers
 * evaluation by chaining through the stored operations back to the source.
 *
 * <p>{@link HeadPipeline} extends this class to serve as the entry point,
 * wrapping a {@link com.custom.stream.operation.intermediate.HeadOperation}
 * that holds the source data array.
 *
 * @param <T> the element type of this pipeline stage
 */
public class OperationalPipeline<T> implements Stream<T> {
    protected final IntermediateOperation<T> intermediateOperation;

    public OperationalPipeline(IntermediateOperation<T> intermediateOperation) {
        this.intermediateOperation = intermediateOperation;
    }

    @Override
    public Stream<T> filter(FilterPredicate<T> filterPredicate) {
        UpstreamSource<T> upstreamSource = intermediateOperation::operate;
        IntermediateOperation<T> newIntermediateOperation = new FilterOperation<>(upstreamSource, filterPredicate);
        return new OperationalPipeline<>(newIntermediateOperation);
    }

    @Override
    public <R> Stream<R> map(MapperPredicate<T, R> mapperPredicate) {
        UpstreamSource<T> upstreamSource = intermediateOperation::operate;
        IntermediateOperation<R> newIntermediateOperation = new MapOperation<>(upstreamSource, mapperPredicate);
        return new OperationalPipeline<>(newIntermediateOperation);
    }

    @Override
    public Stream<T> peek(PeekPredicate<T> peekPredicate) {
        UpstreamSource<T> upstreamSource = intermediateOperation::operate;
        IntermediateOperation<T> newIntermediateOperation = new PeekOperation<>(upstreamSource, peekPredicate);
        return new OperationalPipeline<>(newIntermediateOperation);
    }

    @Override
    public Stream<T> limit(long maxSize) {
        UpstreamSource<T> upstreamSource = intermediateOperation::operate;
        IntermediateOperation<T> newIntermediateOperation = new LimitOperation<>(upstreamSource, maxSize);
        return new OperationalPipeline<>(newIntermediateOperation);
    }

    @Override
    public List<T> toList() {
        UpstreamSource<T> upstreamSource = intermediateOperation::operate;
        TerminalOperation<T, List<T>> newTerminalOperation = new ToListOperation<>();
        return newTerminalOperation.operate(upstreamSource);
    }

    @Override
    public long count() {
        UpstreamSource<T> upstreamSource = intermediateOperation::operate;
        TerminalOperation<T, Long> newTerminalOperation = new CountOperation<>();
        return newTerminalOperation.operate(upstreamSource);
    }
}
