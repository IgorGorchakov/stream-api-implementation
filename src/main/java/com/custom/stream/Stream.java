package com.custom.stream;

import com.custom.stream.pipeline.HeadPipeline;
import com.custom.stream.predicate.FilterPredicate;
import com.custom.stream.predicate.MapperPredicate;
import com.custom.stream.predicate.PeekPredicate;

import java.util.List;

/**
 * A simplified stream interface supporting lazy intermediate operations
 * ({@code filter}, {@code map}, {@code peek}, {@code limit}) and eager terminal
 * operations ({@code toList}, {@code count}).
 *
 * <p>Mirrors the core contract of {@code java.util.stream.Stream} but built from scratch
 * without using {@code java.util.stream} or {@code java.util.function}.
 *
 * @param <T> the type of elements in this stream
 */
public interface Stream<T> {

    /**
     * Returns a stream consisting of the elements that match the given predicate.
     *
     * <p>This is a lazy, stateless intermediate operation.
     *
     * @param filterPredicate the predicate to apply to each element
     * @return a new stream containing only elements that satisfy the predicate
     */
    Stream<T> filter(FilterPredicate<T> filterPredicate);

    /**
     * Returns a stream consisting of the results of applying the given mapper
     * to each element.
     *
     * <p>This is a lazy, stateless intermediate operation.
     *
     * @param mapperPredicate the function to apply to each element
     * @param <R>             the element type of the resulting stream
     * @return a new stream with the transformed elements
     */
    <R> Stream<R> map(MapperPredicate<T, R> mapperPredicate);

    /**
     * Returns a stream that additionally performs the given action on each
     * element as it is consumed.
     *
     * <p>This is a lazy, stateless intermediate operation mainly useful for
     * debugging and logging.
     *
     * @param peekPredicate the action to perform on each element
     * @return a new stream with the same elements
     */
    Stream<T> peek(PeekPredicate<T> peekPredicate);

    /**
     * Returns a stream truncated to at most {@code maxSize} elements.
     *
     * <p>This is a lazy, stateful intermediate operation that short-circuits
     * the pipeline once the limit is reached.
     *
     * @param maxSize the maximum number of elements the stream should contain
     * @return a new stream limited to {@code maxSize} elements
     */
    Stream<T> limit(long maxSize);

    /**
     * Triggers evaluation of the pipeline and collects all resulting elements
     * into a {@link List}.
     *
     * <p>This is an eager terminal operation.
     *
     * @return a list containing the pipeline output
     */
    List<T> toList();

    /**
     * Triggers evaluation of the pipeline and returns the count of elements.
     *
     * <p>This is an eager terminal operation.
     *
     * @return the number of elements in the pipeline
     */
    long count();

    /**
     * Creates a new stream from the given elements.
     *
     * @param elements the elements to stream
     * @param <T>      the element type
     * @return a new stream containing the given elements
     */
    @SafeVarargs
    static <T> Stream<T> of(T... elements) {
        return new HeadPipeline<>(elements);
    }
}
