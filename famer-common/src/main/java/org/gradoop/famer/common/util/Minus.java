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

package org.gradoop.famer.common.util;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


/**
 *
 */

public class Minus<A> {
    public DataSet<A> execute (DataSet<Tuple2<A, String>> first, DataSet<Tuple2<A, String>> second){
        DataSet<Tuple3<A, String, String>> firstSet = first.map(new identify("first"));
        DataSet<Tuple3<A, String, String>> secondSet = second.map(new identify("sec"));
        return firstSet.union(secondSet).groupBy(1).reduceGroup(new reducer());
    }

    private class identify<A> implements MapFunction<Tuple2<A, String>, Tuple3<A, String, String>> {
        private String id;
        public identify(String ID) {
            id = ID;
        }

        @Override
        public Tuple3<A, String, String> map(Tuple2<A, String> value) throws Exception {
            return Tuple3.of(value.f0, value.f1, id);
        }
    }

    private class reducer implements GroupReduceFunction<Tuple3<A, String, String>, A> {
        @Override
        public void reduce(Iterable<Tuple3<A, String, String>> values, Collector<A> out) throws Exception {
            int cnt = 0;
            A a = null;
            String type = "";
            for (Tuple3<A, String, String> v: values) {
                a = v.f0;
                type = v.f2;
                cnt++;
            }
            if (cnt == 1 && type.equals("first"))
                out.collect(a);
        }
    }
}