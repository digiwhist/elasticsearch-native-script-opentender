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

package org.elasticsearch.opentender.nativescript.plugin;

import org.elasticsearch.opentender.nativescript.script.WeightedAvgRangeScript;
import org.elasticsearch.opentender.nativescript.script.weightedavg.CombineScriptFactory;
import org.elasticsearch.opentender.nativescript.script.weightedavg.InitScriptFactory;
import org.elasticsearch.opentender.nativescript.script.weightedavg.MapScriptFactory;
import org.elasticsearch.opentender.nativescript.script.weightedavg.ReduceScriptFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;

/**
 * This class is instantiated when Elasticsearch loads the plugin for the
 * first time. If you change the name of this plugin, make sure to update
 * src/main/resources/es-plugin.properties file that points to this class.
 */
public class NativeScriptOpentenderPlugin extends Plugin {

    /**
     * The name of the plugin.
     * 
     * This name will be used by elasticsearch in the log file to refer to this plugin.
     *
     * @return plugin name.
     */
    @Override
    public String name() {
        return "opentender-plugin";
    }

    /**
     * The description of the plugin.
     *
     * @return plugin description
     */
    @Override
    public String description() {
        return "Opentender Plugins";
    }

    public void onModule(ScriptModule module) {
        //search scripts
        module.registerScript(WeightedAvgRangeScript.SCRIPT_NAME, WeightedAvgRangeScript.Factory.class);
        //aggregation scripts
        module.registerScript("weighted_avg_init", InitScriptFactory.class);
        module.registerScript("weighted_avg_map", MapScriptFactory.class);
        module.registerScript("weighted_avg_combine", CombineScriptFactory.class);
        module.registerScript("weighted_avg_reduce", ReduceScriptFactory.class);
    }
}
