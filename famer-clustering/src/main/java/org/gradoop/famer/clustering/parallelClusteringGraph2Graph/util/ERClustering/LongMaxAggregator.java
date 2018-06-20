package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ERClustering;

/**
 * An {@link Aggregator} that calculate maximum.
 */
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.LongValue;


@SuppressWarnings("serial")
@PublicEvolving
public class LongMaxAggregator implements Aggregator<LongValue> {

    private long max ;	// the max

    public LongValue getAggregate() {
        return new LongValue(max);
    }

    public void aggregate(LongValue element) {
        max = Math.max(element.getValue() , max);
    }

    /**
     * Compares the given value with the current aggregate.
     *
     * @param value The value to compare with the aggregate.
     */
    public void aggregate(long value) {
        max = Math.max(value , max);
    }
    public void aggregate(int value) {
        max = Math.max(value , max);
    }

    public void reset() {
        ////
        max = Long.MIN_VALUE;
    }
}
