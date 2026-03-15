package com.custom.stream.predicate;

/**
 * Functional interface that performs a side-effect action on each element
 * without modifying it. Used by the {@code peek()} intermediate operation.
 *
 * <p>Replaces {@code java.util.function.Consumer<T>} in this custom stream implementation.
 *
 * @param <T> the type of element to observe
 */
@FunctionalInterface
public interface PeekPredicate<T> {

    void peek(T element);
}
