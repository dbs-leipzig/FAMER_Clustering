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

package org.gradoop.famer.common.maxDeltaLinkSelection.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class MakeEdgeWithSelectedStatus2 implements GroupReduceFunction <Tuple3<Edge, String, Integer>, Edge>{

    @Override
    public void reduce(Iterable<Tuple3<Edge, String, Integer>> values, Collector<Edge> out) throws Exception {
        int isSelectedCnt = 0;
        Edge e = new Edge();
        for (Tuple3<Edge, String, Integer> value: values){
            e = value.f0;
            isSelectedCnt += (value.f2);
        }
        if (isSelectedCnt == 2)
            e.setProperty("isSelected", 2);
        else if (isSelectedCnt == 1)
            e.setProperty("isSelected", 1);
        else
            e.setProperty("isSelected", 0);
        out.collect(e);
    }
}
