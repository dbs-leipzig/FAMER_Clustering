/*
 * Copyright © 2016 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
