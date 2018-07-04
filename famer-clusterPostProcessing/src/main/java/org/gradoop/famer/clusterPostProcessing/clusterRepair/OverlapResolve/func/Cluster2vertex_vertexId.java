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

package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class Cluster2vertex_vertexId implements FlatMapFunction <Cluster, Tuple2<Vertex, String>> {
    @Override
    public void flatMap(Cluster in, Collector<Tuple2<Vertex, String>> out) throws Exception {
        for (Vertex v:in.getVertices())
            out.collect(Tuple2.of(v, v.getId().toString()));
    }
}
