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
 * Combine script
 */
public class CombineScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        return new CombineScript(params);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    private static class CombineScript extends AbstractExecutableScript {

        private final Map<String, Object> params;

        public CombineScript(Map<String, Object> params) {
            this.params = params;
        }

        @Override
        public Object run() {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            final Double sum = (Double) agg.get(InitScriptFactory.SUM_FIELD);
            final Double count = (Double) agg.get(InitScriptFactory.COUNT_FIELD);
            final ArrayList<Double> list = new ArrayList<Double>();
            list.add(sum);
            list.add(count);
            return list;
        }
    }
}
