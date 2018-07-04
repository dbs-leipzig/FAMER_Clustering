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

package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class GetClusterVertices implements FlatMapFunction <Cluster, Vertex>{
    @Override
    public void flatMap(Cluster in, Collector<Vertex> out) throws Exception {
        Collection<Vertex> vertices = in.getVertices();
        boolean isCompletePerfect = in.getIsCompletePerfect();
        boolean isPerfect = in.getIsPerfect();
        boolean isPerfectIsolated = in.getIsPerfectIsolated();
        for (Vertex v: vertices) {
            v.setProperty("isPerfect", isPerfect);
            v.setProperty("isCompletePerfect", isCompletePerfect);
            v.setProperty("isPerfectIsolated", isPerfectIsolated);
            out.collect(v);
        }
    }
}
