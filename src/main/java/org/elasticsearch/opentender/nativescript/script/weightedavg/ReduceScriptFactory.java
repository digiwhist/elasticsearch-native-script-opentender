/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.opentender.nativescript.script.weightedavg;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * Reduce script
 */
public class ReduceScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        final ArrayList<ArrayList<Double>> aggs = (ArrayList<ArrayList<Double>>) params.get("_aggs");
        return new ReduceScript(aggs);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    private static class ReduceScript extends AbstractExecutableScript {

        private final ArrayList<ArrayList<Double>> aggs;

        public ReduceScript(ArrayList<ArrayList<Double>> aggs) {
            this.aggs = aggs;
        }

        @Override
        public Object run() {
            double sum = 0;
            double count = 0;
            if (aggs != null) {
                for (ArrayList<Double> t : aggs) {
                    if (t!=null && t.get(0) != null && t.get(1) != null) {
                        sum += t.get(0);
                        count += t.get(1);
                    }
                }
            }
            return count > 0 ? (sum / count) : null;
        }
    }
}
