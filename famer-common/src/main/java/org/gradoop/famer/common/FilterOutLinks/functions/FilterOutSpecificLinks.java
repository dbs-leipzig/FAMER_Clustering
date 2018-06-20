package org.gradoop.famer.common.FilterOutLinks.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class FilterOutSpecificLinks implements FlatMapFunction<Edge, Edge> {
//    public enum IsSelected {
//        WEAK (0), NORMAL (1), STRONG (2), UNKNOWN (3);
//
//        private int numVal;
//
//        IsSelected(int numVal) {
//            this.numVal = numVal;
//        }
//        public IsSelected setValue( int value) {
//            switch (value) {
//                case 0:
//                    return IsSelected.WEAK;
//                case 1:
//                    return IsSelected.NORMAL;
//                case 2:
//                    return IsSelected.STRONG;
//            }
//            return IsSelected.UNKNOWN;
//        }
//    }

    private Integer edgeType;
    public FilterOutSpecificLinks (Integer EdgeType) { edgeType = EdgeType;}
    @Override
    public void flatMap(Edge input, Collector<Edge> out) throws Exception {
        Integer inputEdgeType = Integer.parseInt(input.getPropertyValue("isSelected").toString());
        if (inputEdgeType > edgeType)
            out.collect(input);
    }
}
