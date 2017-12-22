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
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.util.ArrayList;
import java.util.Map;

/**
 * Map script
 */
public class MapScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(final @Nullable Map<String, Object> params) {
        return new MapScript(params);
    }

    public final static Double calculateWeightedAverage(LeafDocLookup doc, ArrayList<String> fields, ArrayList<Number> weights) {
        double sum = 0;
        double count = 0;
        if (fields != null) {
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                MappedFieldType fieldType = doc.mapperService().smartNameFieldType(field);
                if (fieldType != null) {
                    ScriptDocValues.Longs value = (ScriptDocValues.Longs) doc.get(field);
                    if (value.size() > 0) {
                        double weight = 1;
                        double val = value.getValue();
                        if (weights != null) {
                            weight = weights.get(i).doubleValue();
                        }
                        sum += (val * weight);
                        count += weight;
                    }
                }
            }
            if (count > 0) {
                return (sum / count);
            }
        }
        return null;
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    private static class MapScript extends AbstractSearchScript {

        private final Map<String, Object> params;

        public MapScript(Map<String, Object> params) {
            this.params = params;
        }

        @Override
        public Object run() {
            Map<String, Object> agg = (Map<String, Object>) params.get("_agg");
            ArrayList<String> fields = (ArrayList<String>) params.get("fields");
            ArrayList<Number> weights = (ArrayList<Number>) params.get("weights");
            Double value = calculateWeightedAverage(doc(), fields, weights);
            if (value != null) {
                agg.put(InitScriptFactory.SUM_FIELD, (Double) agg.get(InitScriptFactory.SUM_FIELD) + value);
                agg.put(InitScriptFactory.COUNT_FIELD, (Double) agg.get(InitScriptFactory.COUNT_FIELD) + 1);
            }
            return null;
        }
    }
}
