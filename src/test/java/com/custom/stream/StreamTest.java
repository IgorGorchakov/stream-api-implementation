package com.custom.stream;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamTest {

    @Test
    void toListWithoutFilter() {
        List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                .toList();

        assertEquals(List.of(1, 2, 3, 4, 5), result);
    }

    @Test
    void singleFilter() {
        List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6)
                .filter(n -> n % 2 == 0)
                .toList();

        assertEquals(List.of(2, 4, 6), result);
    }

    @Test
    void chainedFilters() {
        List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .filter(n -> n % 2 == 0)   // keep even
                .filter(n -> n > 4)         // keep > 4
                .toList();

        assertEquals(List.of(6, 8, 10), result);
    }

    @Test
    void filterWithStrings() {
        List<String> result = Stream.of("apple", "banana", "avocado", "cherry")
                .filter(s -> s.startsWith("a"))
                .toList();

        assertEquals(List.of("apple", "avocado"), result);
    }

    @Test
    void filterMatchesNothing() {
        List<Integer> result = Stream.of(1, 2, 3)
                .filter(n -> n > 100)
                .toList();

        assertEquals(List.of(), result);
    }

    @Test
    void filterMatchesEverything() {
        List<Integer> result = Stream.of(1, 2, 3)
                .filter(n -> n > 0)
                .toList();

        assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    void mapOnly() {
        List<Integer> result = Stream.of(1, 2, 3)
                .map(n -> n * 10)
                .toList();

        assertEquals(List.of(10, 20, 30), result);
    }

    @Test
    void mapChangesType() {
        List<String> result = Stream.of(1, 2, 3)
                .map(n -> "num" + n)
                .toList();

        assertEquals(List.of("num1", "num2", "num3"), result);
    }

    @Test
    void filterThenMap() {
        List<String> result = Stream.of(1, 2, 3, 4, 5, 6)
                .filter(n -> n % 2 == 0)
                .map(String::valueOf)
                .toList();

        assertEquals(List.of("2", "4", "6"), result);
    }

    @Test
    void mapThenFilter() {
        List<Integer> result = Stream.of("apple", "banana", "avocado", "cherry")
                .map(String::length)
                .filter(len -> len > 5)
                .toList();

        assertEquals(List.of(6, 7, 6), result);
    }

    @Test
    void chainedMaps() {
        List<String> result = Stream.of(1, 2, 3)
                .map(n -> n * 2)
                .map(n -> n + 1)
                .map(String::valueOf)
                .toList();

        assertEquals(List.of("3", "5", "7"), result);
    }

    @Test
    void filterMapFilterMap() {
        List<String> result = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .filter(n -> n % 2 == 0)       // 2, 4, 6, 8, 10
                .map(n -> n * 3)               // 6, 12, 18, 24, 30
                .filter(n -> n > 10)           // 12, 18, 24, 30
                .map(n -> "v" + n)             // "v12", "v18", "v24", "v30"
                .toList();

        assertEquals(List.of("v12", "v18", "v24", "v30"), result);
    }

    @Test
    void peekObservesElements() {
        List<Integer> seen = new ArrayList<>();

        List<Integer> result = Stream.of(1, 2, 3)
                .peek(seen::add)
                .toList();

        assertEquals(List.of(1, 2, 3), result);
        assertEquals(List.of(1, 2, 3), seen);
    }

    @Test
    void peekAfterFilter() {
        List<Integer> seen = new ArrayList<>();

        List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6)
                .filter(n -> n % 2 == 0)
                .peek(seen::add)
                .toList();

        assertEquals(List.of(2, 4, 6), result);
        assertEquals(List.of(2, 4, 6), seen);
    }

    @Test
    void peekBeforeFilter() {
        List<Integer> seen = new ArrayList<>();

        List<Integer> result = Stream.of(1, 2, 3, 4)
                .peek(seen::add)
                .filter(n -> n > 2)
                .toList();

        assertEquals(List.of(3, 4), result);
        assertEquals(List.of(1, 2, 3, 4), seen);
    }

    @Test
    void peekBetweenMapAndFilter() {
        List<String> seen = new ArrayList<>();

        List<String> result = Stream.of(1, 2, 3, 4, 5)
                .map(n -> "v" + n)
                .peek(seen::add)
                .filter(s -> s.compareTo("v3") > 0)
                .toList();

        assertEquals(List.of("v4", "v5"), result);
        assertEquals(List.of("v1", "v2", "v3", "v4", "v5"), seen);
    }

    @Test
    void countAll() {
        long result = Stream.of(1, 2, 3, 4, 5)
                .count();

        assertEquals(5, result);
    }

    @Test
    void countAfterFilter() {
        long result = Stream.of(1, 2, 3, 4, 5, 6)
                .filter(n -> n % 2 == 0)
                .count();

        assertEquals(3, result);
    }

    @Test
    void countAfterMapAndFilter() {
        long result = Stream.of("apple", "banana", "avocado", "cherry")
                .map(String::length)
                .filter(len -> len > 5)
                .count();

        assertEquals(3, result);
    }

    @Test
    void countEmpty() {
        long result = Stream.of(1, 2, 3)
                .filter(n -> n > 100)
                .count();

        assertEquals(0, result);
    }

    @Test
    void limitOnly() {
        List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                .limit(3)
                .toList();

        assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    void limitMoreThanAvailable() {
        List<Integer> result = Stream.of(1, 2, 3)
                .limit(10)
                .toList();

        assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    void limitZero() {
        List<Integer> result = Stream.of(1, 2, 3)
                .limit(0)
                .toList();

        assertEquals(List.of(), result);
    }

    @Test
    void filterThenLimit() {
        List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6, 7, 8)
                .filter(n -> n % 2 == 0)
                .limit(2)
                .toList();

        assertEquals(List.of(2, 4), result);
    }

    @Test
    void limitThenMap() {
        List<String> result = Stream.of(1, 2, 3, 4, 5)
                .limit(3)
                .map(n -> "v" + n)
                .toList();

        assertEquals(List.of("v1", "v2", "v3"), result);
    }

    @Test
    void limitThenFilter() {
        List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                .limit(4)
                .filter(n -> n % 2 == 0)
                .toList();

        assertEquals(List.of(2, 4), result);
    }

    @Test
    void limitShortCircuits() {
        List<Integer> seen = new ArrayList<>();

        List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                .peek(seen::add)
                .limit(3)
                .toList();

        assertEquals(List.of(1, 2, 3), result);
        assertEquals(List.of(1, 2, 3), seen);
    }

    @Test
    void countAfterLimit() {
        long result = Stream.of(1, 2, 3, 4, 5)
                .limit(3)
                .count();

        assertEquals(3, result);
    }
}
