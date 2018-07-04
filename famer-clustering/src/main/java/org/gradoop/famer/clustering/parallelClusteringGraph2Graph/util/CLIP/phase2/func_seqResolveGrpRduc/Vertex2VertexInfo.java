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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class Vertex2VertexInfo implements MapFunction <Vertex, Tuple3<String, String, String>>{
    @Override
    public Tuple3<String, String, String> map(Vertex vertex) throws Exception {
        return Tuple3.of(vertex.getId().toString(), vertex.getPropertyValue("srcId").toString(),
                vertex.getPropertyValue("ClusterId").toString());
    }
}
