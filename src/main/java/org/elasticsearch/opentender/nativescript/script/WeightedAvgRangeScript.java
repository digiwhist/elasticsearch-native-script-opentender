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

package org.elasticsearch.opentender.nativescript.script;

import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.script.ScriptException;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import static org.elasticsearch.opentender.nativescript.script.weightedavg.MapScriptFactory.calculateWeightedAverage;

/**
 * Script that filters a by a range with a calculated weighted average
 */
public class WeightedAvgRangeScript extends AbstractSearchScript {

    ArrayList<String> fields;
    ArrayList<Number> weights;
    Double lte;
    Double gte;

    final static public String SCRIPT_NAME = "weighted_avg_range";

    /**
     * Factory that is registered in
     * {@link org.elasticsearch.opentender.nativescript.plugin.NativeScriptOpentenderPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory implements NativeScriptFactory {

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new WeightedAvgRangeScript(params);
        }

        /**
         * Indicates if document scores may be needed by the produced scripts.
         *
         * @return {@code true} if scores are needed.
         */
        @Override
        public boolean needsScores() {
            return false;
        }
    }

    /**
     * @param params terms that a scored are placed in this parameter. Initialize
     *               them here.
     * @throws ScriptException
     */
    private WeightedAvgRangeScript(Map<String, Object> params) throws ScriptException {
        params.entrySet();
        fields = (ArrayList<String>) params.get("fields");
        weights = (ArrayList<Number>) params.get("weights");
        Number _lte = (Number) params.get("lte");
        Number _gte = (Number) params.get("gte");
        if (fields == null || weights == null || _gte == null || _lte == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": gte, lte, fields or weights parameter missing!");
        }
        if (weights.size() != fields.size()) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": fields and weights array must have same length!");
        }
        lte = _lte.doubleValue();
        gte = _gte.doubleValue();
    }

    @Override
    public Object run() {
        Double value = calculateWeightedAverage(doc(), fields, weights);
        if (value != null) {
            return (value >= gte) && (value <= lte);
        }
        return false;
    }

}
