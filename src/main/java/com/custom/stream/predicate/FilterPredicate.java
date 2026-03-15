package com.custom.stream.predicate;

/**
 * Functional interface that tests whether an element satisfies a condition.
 * Used by the {@code filter()} intermediate operation.
 *
 * <p>Replaces {@code java.util.function.Predicate<T>} in this custom stream implementation.
 *
 * @param <T> the type of element to test
 */
@FunctionalInterface
public interface FilterPredicate<T> {

    boolean check(T element);
}
