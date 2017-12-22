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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

/**
 * Test if the computed tfidf in NaiveTFIDFScoreScript equals 0.0 for each
 * document as would be expected if each document in the index contains only one
 * and always the same term.
 */
public class WeightedAvgRangeScriptTests extends AbstractSearchScriptTestCase {

    @SuppressWarnings("unchecked")
    @Test
    public void testWeightedAvgScore() throws Exception {

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
        params.put("lte", 100);
        params.put("gte", 65);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(
                        QueryBuilders.boolQuery().filter(
                                QueryBuilders.scriptQuery(new Script(WeightedAvgRangeScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params))
                        )
                )
                .setSize(0)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 3 hits
        assertHitCount(searchResponse, 3);
    }

    @Test
    public void testWeightedAvgScore2() throws Exception {

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
        weights.add(1.0);
        weights.add(1.0);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fields", fields);
        params.put("weights", weights);
        params.put("lte", 100);
        params.put("gte", 65);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(
                        QueryBuilders.boolQuery().filter(
                                QueryBuilders.scriptQuery(new Script(WeightedAvgRangeScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params))
                        )
                )
                .setSize(0)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 3 hits
        assertHitCount(searchResponse, 2);
    }
    @Test
    public void testWeightedAvgScore3() throws Exception {

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
        weights.add(1.0);
        weights.add(1.0);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fields", fields);
        params.put("weights", weights);
        params.put("lte", 10);
        params.put("gte", 10);

        // Find profit from all transaction
        SearchResponse searchResponse = client().prepareSearch("transactions")
                .setTypes("stock")
                .setQuery(
                        QueryBuilders.boolQuery().filter(
                                QueryBuilders.scriptQuery(new Script(WeightedAvgRangeScript.SCRIPT_NAME, ScriptService.ScriptType.INLINE, "native", params))
                        )
                )
                .setSize(0)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        // There should be 3 hits
        assertHitCount(searchResponse, 1);
    }


}
