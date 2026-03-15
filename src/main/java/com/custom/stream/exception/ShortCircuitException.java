package com.custom.stream.exception;

/**
 * Thrown by stateful operations (e.g. {@code limit}) to signal that the pipeline
 * should stop pushing elements from the source.
 *
 * <p>Caught by {@link com.custom.stream.operation.intermediate.HeadOperation}
 * to terminate source iteration early. Overrides {@link #fillInStackTrace()}
 * to avoid the cost of capturing a stack trace, since this exception is used
 * purely as a control-flow mechanism.
 */
public class ShortCircuitException extends RuntimeException {

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
