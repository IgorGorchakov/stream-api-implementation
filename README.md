# Custom Stream Implementation

The real Java Stream uses a linked pipeline of AbstractPipeline stages (ReferencePipeline), where each stage holds a reference to the previous stage and wraps behavior via `Sink` chaining. It's the stage-based approach.

#### Real Java Stream internals:
- Uses a linked pipeline of stages — AbstractPipeline → ReferencePipeline with subclasses like Head, StatelessOp, etc.
- Each intermediate operation creates a new stage object that points back to the previous stage (linked list, not a flat list)
- Each stage knows its upstream reference
- At terminal time, it walks the chain backwards and builds a Sink pipeline (similar to DownstreamSource wrapping)
- The Head stage holds the source data — just like HeadPipeline

A from-scratch implementation of Java's Stream API concept, built without using `java.util.stream`
or `java.util.function`. Demonstrates how lazy evaluation pipelines work internally.

## Usage

```java
List<String> result = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .filter(n -> n % 2 == 0)       // keep even numbers
        .map(n -> n * 3)               // triple them
        .peek(n -> System.out.println("processing: " + n))
        .filter(n -> n > 10)           // keep only > 10
        .limit(3)                      // take first 3 matches
        .map(n -> "v" + n)             // convert to string
        .toList();                     // triggers the pipeline

// result: ["v12", "v18", "v24"]
```

## Operations

| Operation   | Type                     | Description                                      |
|-------------|--------------------------|--------------------------------------------------|
| `filter()`  | Intermediate (stateless) | Keeps elements that satisfy the predicate        |
| `map()`     | Intermediate (stateless) | Transforms each element (can change the type)    |
| `peek()`    | Intermediate (stateless) | Observes each element via side-effect            |
| `limit(n)`  | Intermediate (stateful)  | Truncates the stream to at most `n` elements     |
| `toList()`  | Terminal                 | Triggers evaluation, returns a `List<T>`         |
| `count()`   | Terminal                 | Triggers evaluation, returns the element count   |

**Intermediate** operations are lazy — they record *what* to do but execute nothing.
**Terminal** operations trigger the entire pipeline, pushing data from source to result.

## Architecture: Chain of Pipeline Objects

The stream is designed as a **chain of pipeline objects**. Each intermediate operation
creates a new `OperationalPipeline` that wraps an `IntermediateOperation` object, forming a linked chain from
source to the final operation. No data moves until a terminal operation is called.

```
  .of(1,2,3,4,5,6)       .filter(even?)           .map(n -> "val"+n)       .peek(println)
  +--------------+       +------------------+       +------------------+       +------------------+
  | HeadPipeline | <---- | Operational      | <---- | Operational      | <---- | Operational      |
  |              |       | Pipeline         |       | Pipeline         |       | Pipeline         |
  | source:      |       | operation:       |       | operation:       |       | operation:       |
  | [1,2,3,4,5,6]|       |  FilterOperation |       |  MapOperation    |       |  PeekOperation   |
  |              |       |  predicate:      |       |  mapper:         |       |  action:         |
  |              |       |   n % 2 == 0     |       |   "val" + n      |       |   println(n)     |
  +--------------+       +------------------+       +------------------+       +------------------+
        ^                      ^                          ^                          ^
        |                      |                          |                          |
      SOURCE              wraps source               wraps filter              wraps map
```

Each `IntermediateOperation` stores an `UpstreamSource` (a reference to the previous stage's
`operate` method). When a terminal operation like `toList()` is called
on the last stage, it triggers a cascade back through the chain to the source.

### OperationalPipeline — Eliminating Duplication

`HeadPipeline` extends `OperationalPipeline<T>`, which provides shared implementations of every
intermediate and terminal operation. Each pipeline stage only needs to supply
an `IntermediateOperation` that describes how that stage processes and forwards
elements:

| Pipeline                       | Role                                                                  |
|--------------------------------|-----------------------------------------------------------------------|
| `HeadPipeline`                 | Source stage — wraps a `HeadOperation` that iterates the source array |
| `OperationalPipeline`          | Base stage — holds an `IntermediateOperation` and implements all stream operations |

The actual processing logic lives in the `IntermediateOperation` implementations:

| Operation          | `operate()` logic                                            |
|--------------------|--------------------------------------------------------------|
| `FilterOperation`  | Forwards only elements passing the predicate                 |
| `MapOperation`     | Applies the mapper, forwards the transformed result          |
| `PeekOperation`    | Runs the side-effect action, forwards element unchanged      |
| `LimitOperation`   | Forwards at most `n` elements, then short-circuits           |

Adding a new **intermediate** operation means creating one small `IntermediateOperation` class and (optionally) a predicate interface.
Adding a new **terminal** operation means creating a `TerminalOperation` class and adding one method to `OperationalPipeline` — all
stages inherit it automatically.

### Short-Circuiting

Stateful operations like `limit(n)` need to stop the pipeline early. When a `LimitOperation`
has forwarded enough elements, it throws a `ShortCircuitException` (an unchecked exception
with no stack trace overhead). `HeadOperation` catches it and stops iterating the source.
This is analogous to the `cancel` flag mechanism in the real Java Stream API, adapted for
our push-based architecture where there is no polling point between elements.

### Data Flow: Push-Based, One Element at a Time

When `toList()` triggers the pipeline, each element flows through the **entire chain**
before the next element enters:

```
  HeadPipeline       Operational         Operational        Operational        toList()
 [1,2,3,4,5,6]    Pipeline             Pipeline           Pipeline           result list
                   FilterOperation      MapOperation       PeekOperation
                    n % 2 == 0?        n -> "val"+n       println(n)
      |                  |                  |                  |                  |
      |--- 1 ----------->|                  |                  |                  |
      |              REJECTED               |                  |                  |
      |                                     |                  |                  |
      |--- 2 ----------->|                  |                  |                  |
      |               PASSED                |                  |                  |
      |               2 ------------------->|                  |                  |
      |                              "val2" ------------------>|                  |
      |                                               print "val2"               |
      |                                                "val2" ------------------>|
      |                                                                   [.., "val2"]
      |--- 3 ----------->|                  |                  |                  |
      |              REJECTED               |                  |                  |
      |                                     |                  |                  |
      |--- 4 ----------->|                  |                  |                  |
      |               PASSED                |                  |                  |
      |               4 ------------------->|                  |                  |
      |                              "val4" ------------------>|                  |
      |                                               print "val4"               |
      |                                                "val4" ------------------>|
      |                                                                   [.., "val4"]
      |                                     ...                                  |
```

Element `1` is fully processed (and rejected) before element `2` even enters the pipeline.

### UpstreamSource and DownstreamSource

The pipeline stages are connected through two interfaces that represent the direction of data flow:

```
 UpstreamSource<T>                       DownstreamSource<T>
   void pushToDownstream(               void process(T element)
        DownstreamSource)                    ^
        |                                    |
        |   "iterate your elements           |   "here's one element
        |    and push each one     --------->|    for you to process"
        |    to this downstream"             |
```

Each `IntermediateOperation` stores an `UpstreamSource` (a reference to the previous stage's
`operate` method) and uses it during execution to pull elements from upstream.
`OperationalPipeline` delegates to its `IntermediateOperation`, which calls its `UpstreamSource` to trigger the
upstream stage. This creates the chain:

```
HeadPipeline                OperationalPipeline           OperationalPipeline
  HeadOperation      <--upstreamSource--  FilterOperation     <--upstreamSource--  MapOperation
  .operate()                              .operate()                               .operate()
```

When `toList()` calls the last stage's operation, it triggers a
cascade back to the source.

## Project Structure

```
src/main/java/com/custom/stream/
    |
    |-- Stream.java                     Public interface (of, filter, map, peek, limit, toList, count)
    |
    |-- predicate/
    |       |-- FilterPredicate.java    boolean check(T) — used by filter()
    |       |-- MapperPredicate.java    R map(T)         — used by map()
    |       |-- PeekPredicate.java      void peek(T)     — used by peek()
    |
    |-- exception/
    |       |-- ShortCircuitException.java  Signals early pipeline termination (used by limit)
    |
    |-- pipeline/
    |       |-- UpstreamSource.java           Pushes elements to a DownstreamSource
    |       |-- DownstreamSource.java         Receives a single element
    |       |-- OperationalPipeline.java      Base class — shared intermediate & terminal operations
    |       |-- HeadPipeline.java             Source stage, holds the data array
    |
    |-- operation/
            |-- intermediate/
            |       |-- IntermediateOperation.java  Interface for intermediate operation implementations
            |       |-- HeadOperation.java          Source operation, iterates the data array
            |       |-- FilterOperation.java        Drops elements that fail the predicate
            |       |-- MapOperation.java           Transforms elements from one type to another
            |       |-- PeekOperation.java          Observes elements without modifying them
            |       |-- LimitOperation.java         Takes the first n elements, then short-circuits
            |
            |-- terminal/
                    |-- TerminalOperation.java      Interface for terminal operation implementations
                    |-- ToListOperation.java         Collects elements into a List
                    |-- CountOperation.java          Counts the number of elements

src/test/java/com/custom/stream/
    |-- StreamTest.java                 28 tests covering all operations and combinations
```

## Custom Functional Interfaces

This project intentionally avoids `java.util.function.*`. Instead, it defines its own:

| Custom Interface          | Replaces                             | Method                           |
|---------------------------|--------------------------------------|----------------------------------|
| `FilterPredicate<T>`      | `java.util.function.Predicate<T>`    | `boolean check(T)`              |
| `MapperPredicate<T, R>`   | `java.util.function.Function<T, R>`  | `R map(T)`                      |
| `PeekPredicate<T>`        | `java.util.function.Consumer<T>`     | `void peek(T)`                  |
| `UpstreamSource<T>`       | --                                   | `void pushToDownstream(DownstreamSource<T>)` |
| `DownstreamSource<T>`     | --                                   | `void process(T)`               |

All are `@FunctionalInterface`, so lambda expressions and method references work seamlessly.

## Building and Testing

```bash
mvn test
```

Runs 28 tests covering all operations and their combinations.
