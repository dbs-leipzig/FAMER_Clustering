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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class GetClusterEdges implements FlatMapFunction <Cluster, Edge>{
    @Override
    public void flatMap(Cluster in, Collector<Edge> out) throws Exception {
        Collection<Edge> interLinks = in.getInterLinks();
        for (Edge e:interLinks)
            out.collect(e);
        Collection<Edge> intraLinks = in.getIntraLinks();
        for (Edge e : intraLinks)
            out.collect(e);
    }
}
