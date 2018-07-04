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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class Edge2EdgeInfo implements MapFunction <Edge, Tuple4<String, String, String, Double>>{
    private Double simValueCoef;
    private Double strengthCoef;

    public Edge2EdgeInfo(Double inputSimValueCoef, Double inputStrengthCoef){
        simValueCoef = inputSimValueCoef;
        strengthCoef = inputStrengthCoef;
    }

    @Override
    public Tuple4<String, String, String, Double> map(Edge edge) throws Exception {
        Double prioValue = simValueCoef*Double.parseDouble(edge.getPropertyValue("value").toString())
                +strengthCoef*Integer.parseInt(edge.getPropertyValue("isSelected").toString());
        return Tuple4.of(edge.getId().toString(), edge.getSourceId().toString(), edge.getTargetId().toString(), prioValue);
    }
}
