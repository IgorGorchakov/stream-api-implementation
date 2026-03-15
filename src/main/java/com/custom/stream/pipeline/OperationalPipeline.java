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
    protected final IntermediateOperation<T> previousIntermediateOperation;

    public OperationalPipeline(IntermediateOperation<T> previousIntermediateOperation) {
        this.previousIntermediateOperation = previousIntermediateOperation;
    }

    @Override
    public Stream<T> filter(FilterPredicate<T> filterPredicate) {
        UpstreamSource<T> upstreamSource = previousIntermediateOperation::operate;
        IntermediateOperation<T> currentIntermediateOperation = new FilterOperation<>(upstreamSource, filterPredicate);
        return new OperationalPipeline<>(currentIntermediateOperation);
    }

    @Override
    public <R> Stream<R> map(MapperPredicate<T, R> mapperPredicate) {
        UpstreamSource<T> upstreamSource = previousIntermediateOperation::operate;
        IntermediateOperation<R> currentIntermediateOperation = new MapOperation<>(upstreamSource, mapperPredicate);
        return new OperationalPipeline<>(currentIntermediateOperation);
    }

    @Override
    public Stream<T> peek(PeekPredicate<T> peekPredicate) {
        UpstreamSource<T> upstreamSource = previousIntermediateOperation::operate;
        IntermediateOperation<T> currentIntermediateOperation = new PeekOperation<>(upstreamSource, peekPredicate);
        return new OperationalPipeline<>(currentIntermediateOperation);
    }

    @Override
    public Stream<T> limit(long maxSize) {
        UpstreamSource<T> upstreamSource = previousIntermediateOperation::operate;
        IntermediateOperation<T> currentIntermediateOperation = new LimitOperation<>(upstreamSource, maxSize);
        return new OperationalPipeline<>(currentIntermediateOperation);
    }

    @Override
    public List<T> toList() {
        TerminalOperation<T, List<T>> actualTerminalOperation = new ToListOperation<>();
        UpstreamSource<T> upstreamSource = previousIntermediateOperation::operate;
        return actualTerminalOperation.operate(upstreamSource);
    }

    @Override
    public long count() {
        TerminalOperation<T, Long> actualTerminalOperation = new CountOperation<>();
        UpstreamSource<T> upstreamSource = previousIntermediateOperation::operate;
        return actualTerminalOperation.operate(upstreamSource);
    }
}
