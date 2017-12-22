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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.scriptedMetric;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;

/**
 * Scripted Metric Agg Testss
 */
public class ScriptedMetricAggsScriptTests extends AbstractSearchScriptTestCase {

    @SuppressWarnings("unchecked")
    @Test
    public void testScriptedMetricAggs() throws Exception {

        // Create a new lookup index
        String stockMapping = XContentFactory.jsonBuilder().startObject().startObject("stock")
                .startObject("properties")
                .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("value1").field("type", "long").endObject()
                .startObject("value2").field("type", "long").endObject()
                .endObject().endObject().endObject()
                .string();

        assertAcked(prepareCreate("transactions")
                .addMapping("stock", stockMapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index stock records:
        indexBuilders.add(client().prepareIndex("transactions", "stock", "1").setSource("type", "sale", "value1", 40, "value2", 80)); // 60
        indexBuilders.add(client().prepareIndex("transactions", "stock", "2").setSource("type", "cost", "value1", 10));  // 10
        indexBuilders.add(client().prepareIndex("transactions", "stock", "3").setSource("type", "cost", "value1", 30, "value2", 100)); // 65
        indexBuilders.add(client().prepareIndex("transactions", "stock", "4").setSource("type", "sale", "value1", 130, "value2", 50)); // 90

        indexRandom(true, indexBuilders);

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("value1");
        fields.add("value2");
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fields", fields);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(matchAllQuery())
                .setSize(0)
                .addAggregation(scriptedMetric("profit")
                        .params(params)
                        .initScript(new Script("weighted_avg_init", ScriptService.ScriptType.INLINE, "native", null))
                        .mapScript(new Script("weighted_avg_map", ScriptService.ScriptType.INLINE, "native", null))
                        .combineScript(new Script("weighted_avg_combine", ScriptService.ScriptType.INLINE, "native", null))
                        .reduceScript(new Script("weighted_avg_reduce", ScriptService.ScriptType.INLINE, "native", null)))
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 4 hits - we are running aggs on everything
        assertHitCount(searchResponse, 4);

        // The avg should be 56.25
        assertThat((Double) searchResponse.getAggregations().get("profit").getProperty("value"), equalTo(56.25));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScriptedMetricEqualWeighedAggs() throws Exception {

        // Create a new lookup index
        String stockMapping = XContentFactory.jsonBuilder().startObject().startObject("stock")
                .startObject("properties")
                .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("value1").field("type", "long").endObject()
                .startObject("value2").field("type", "long").endObject()
                .endObject().endObject().endObject()
                .string();

        assertAcked(prepareCreate("transactions")
                .addMapping("stock", stockMapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index stock records:
        indexBuilders.add(client().prepareIndex("transactions", "stock", "1").setSource("type", "sale", "value1", 40, "value2", 80)); // 60
        indexBuilders.add(client().prepareIndex("transactions", "stock", "2").setSource("type", "cost", "value1", 10));  // 10
        indexBuilders.add(client().prepareIndex("transactions", "stock", "3").setSource("type", "cost", "value1", 30, "value2", 100)); // 65
        indexBuilders.add(client().prepareIndex("transactions", "stock", "4").setSource("type", "sale", "value1", 130, "value2", 50)); // 90

        indexRandom(true, indexBuilders);

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("value1");
        fields.add("value2");
        ArrayList<Number> weights = new ArrayList<Number>();
        weights.add(1);
        weights.add(1.0);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fields", fields);
        params.put("weights", weights);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(matchAllQuery())
                .setSize(0)
                .addAggregation(scriptedMetric("profit")
                        .params(params)
                        .initScript(new Script("weighted_avg_init", ScriptService.ScriptType.INLINE, "native", null))
                        .mapScript(new Script("weighted_avg_map", ScriptService.ScriptType.INLINE, "native", null))
                        .combineScript(new Script("weighted_avg_combine", ScriptService.ScriptType.INLINE, "native", null))
                        .reduceScript(new Script("weighted_avg_reduce", ScriptService.ScriptType.INLINE, "native", null)))
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 4 hits - we are running aggs on everything
        assertHitCount(searchResponse, 4);

        // The avg should be 56.25
        assertThat((Double) searchResponse.getAggregations().get("profit").getProperty("value"), equalTo(56.25));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScriptedMetricWeighedAggs() throws Exception {

        // Create a new lookup index
        String stockMapping = XContentFactory.jsonBuilder().startObject().startObject("stock")
                .startObject("properties")
                .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("value1").field("type", "long").endObject()
                .startObject("value2").field("type", "long").endObject()
                .endObject().endObject().endObject()
                .string();

        assertAcked(prepareCreate("transactions")
                .addMapping("stock", stockMapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Index stock records:
        indexBuilders.add(client().prepareIndex("transactions", "stock", "1").setSource("type", "sale", "value1", 40, "value2", 80)); // 66.66666666666667
        indexBuilders.add(client().prepareIndex("transactions", "stock", "2").setSource("type", "cost", "value1", 10));  // 10
        indexBuilders.add(client().prepareIndex("transactions", "stock", "3").setSource("type", "cost", "value1", 30, "value2", 100)); // 76.66666666666667
        indexBuilders.add(client().prepareIndex("transactions", "stock", "4").setSource("type", "sale", "value1", 130, "value2", 50)); // 76.66666666666667

        indexRandom(true, indexBuilders);

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("value1");
        fields.add("value2");
        ArrayList<Number> weights = new ArrayList<Number>();
        weights.add(0.5);
        weights.add(1.0);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fields", fields);
        params.put("weights", weights);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(matchAllQuery())
                .setSize(0)
                .addAggregation(scriptedMetric("profit")
                        .params(params)
                        .initScript(new Script("weighted_avg_init", ScriptService.ScriptType.INLINE, "native", null))
                        .mapScript(new Script("weighted_avg_map", ScriptService.ScriptType.INLINE, "native", null))
                        .combineScript(new Script("weighted_avg_combine", ScriptService.ScriptType.INLINE, "native", null))
                        .reduceScript(new Script("weighted_avg_reduce", ScriptService.ScriptType.INLINE, "native", null)))
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 4 hits - we are running aggs on everything
        assertHitCount(searchResponse, 4);

        // The avg should be ( ((40*0.5)+(80*1))/(0.5+1) + ((10*0.5))/(0.5) + ((30*0.5)+(100*1))/(0.5+1) + ( ((130*0.5)+(50*1))/(0.5+1) ) ) / 4
        assertThat((Double) searchResponse.getAggregations().get("profit").getProperty("value"), equalTo(57.5));
    }

  @SuppressWarnings("unchecked")
    @Test
    public void testScriptedMetricNullAggs() throws Exception {

        // Create a new lookup index
        String stockMapping = XContentFactory.jsonBuilder().startObject().startObject("stock")
                .startObject("properties")
                .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("value1").field("type", "long").endObject()
                .startObject("value2").field("type", "long").endObject()
                .endObject().endObject().endObject()
                .string();

        assertAcked(prepareCreate("transactions")
                .addMapping("stock", stockMapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();

        indexRandom(true, indexBuilders);

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("value1");
        fields.add("value2");
        ArrayList<Number> weights = new ArrayList<Number>();
        weights.add(1);
        weights.add(1.0);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fields", fields);
        params.put("weights", weights);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(matchAllQuery())
                .setSize(0)
                .addAggregation(scriptedMetric("profit")
                        .params(params)
                        .initScript(new Script("weighted_avg_init", ScriptService.ScriptType.INLINE, "native", null))
                        .mapScript(new Script("weighted_avg_map", ScriptService.ScriptType.INLINE, "native", null))
                        .combineScript(new Script("weighted_avg_combine", ScriptService.ScriptType.INLINE, "native", null))
                        .reduceScript(new Script("weighted_avg_reduce", ScriptService.ScriptType.INLINE, "native", null)))
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 0 hits
        assertHitCount(searchResponse, 0);

        assertThat(searchResponse.getAggregations().get("profit").getProperty("value"), equalTo(null));
    }


}
