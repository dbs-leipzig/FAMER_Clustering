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

package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class reduceTuple2toSet implements ReduceFunction<Tuple2<Long, Long>> {
    private int index;
    public reduceTuple2toSet (int Index){
        index = Index;
    }
    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> in1, Tuple2<Long, Long> in2) throws Exception {
        if (index ==0)
            return Tuple2.of(in1.f0+in2.f0,0l);
        else
            return Tuple2.of(0l,in1.f1+in2.f1);
    }
}
