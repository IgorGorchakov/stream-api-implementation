package com.custom.stream.predicate;

/**
 * Functional interface that transforms an element from one type to another.
 * Used by the {@code map()} intermediate operation.
 *
 * <p>Replaces {@code java.util.function.Function<T, R>} in this custom stream implementation.
 *
 * @param <T> the input element type
 * @param <R> the output element type
 */
@FunctionalInterface
public interface MapperPredicate<T, R> {

    R map(T element);
}
