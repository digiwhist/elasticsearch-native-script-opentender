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
import java.util.HashMap;

/**
 * Init script
 */
public class InitScriptFactory implements NativeScriptFactory {

    public static final String SUM_FIELD = "sum";
    public static final String COUNT_FIELD = "count";

    @Override
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        return new AbstractExecutableScript() {
            @Override
            public Object run() {
                double sum = 0;
                double count = 0;
                Map<String, Object> agg =  ((Map<String, Object>)params.get("_agg"));
                if (agg == null) {
                    agg = new HashMap<String, Object>();
                    params.put("_agg", agg);
                }
                agg.put(SUM_FIELD, sum);
                agg.put(COUNT_FIELD, count);
                return null;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }
}
