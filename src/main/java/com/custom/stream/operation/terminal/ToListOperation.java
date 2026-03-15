package com.custom.stream.operation.terminal;

import com.custom.stream.pipeline.UpstreamSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Terminal operation that collects all pipeline elements into an {@link ArrayList}.
 *
 * <p>Triggers pipeline evaluation by pushing all elements into a result list
 * via {@link UpstreamSource#pushToDownstream(com.custom.stream.pipeline.DownstreamSource)}.
 *
 * @param <T> the element type
 */
public class ToListOperation<T> implements TerminalOperation<T, List<T>> {

    @Override
    public List<T> operate(UpstreamSource<T> upstreamSource) {
        List<T> result = new ArrayList<>();
        upstreamSource.pushToDownstream(result::add);
        return result;
    }
}
