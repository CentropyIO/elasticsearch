/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Information about plugins and modules
 */
public class PluginsAndModules implements Writeable, ToXContent {
    private final List<PluginInfo> plugins;
    private final List<PluginInfo> modules;

    public PluginsAndModules(List<PluginInfo> plugins, List<PluginInfo> modules) {
        this.plugins = Collections.unmodifiableList(plugins);
        this.modules = Collections.unmodifiableList(modules);
    }

    public PluginsAndModules(StreamInput in) throws IOException {
        if(in.getVersion().before(Version.V_5_0_0_alpha1)){
            ArrayList<PluginInfo> pluginsList = new ArrayList<>();
            int pluginsSize = in.readInt();
            for (int i = 0; i < pluginsSize; i++) {
                pluginsList.add(new PluginInfo(in));
            }
            this.plugins = Collections.unmodifiableList(pluginsList);
            
            ArrayList<PluginInfo> modulesList = new ArrayList<>();
            if (in.getVersion().onOrAfter(Version.V_2_2_0)) {
                int modulesSize = in.readInt();
                for (int i = 0; i < modulesSize; i++) {
                    modulesList.add(new PluginInfo(in));
                }
            }
            this.modules = Collections.unmodifiableList(modulesList);
        }else{
            this.plugins = Collections.unmodifiableList(in.readList(PluginInfo::new));
            this.modules = Collections.unmodifiableList(in.readList(PluginInfo::new));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_5_0_0_alpha1)) {
            if (out.getVersion().before(Version.V_2_2_0)) {
                // Before 2.2.0, write combined size and then all plugins followed by all modules
                out.writeInt(plugins.size() + modules.size());
                for (PluginInfo plugin : getPluginInfos()) {
                    plugin.writeTo(out);
                }
                for (PluginInfo module : getModuleInfos()) {
                    module.writeTo(out);
                }
            } else {
                // Between 2.2.0 and 5.0.0_alpha1, write plugins and modules separately
                out.writeInt(plugins.size());
                for (PluginInfo plugin : getPluginInfos()) {
                    plugin.writeTo(out);
                }
                out.writeInt(modules.size());
                for (PluginInfo module : getModuleInfos()) {
                    module.writeTo(out);
                }
            }
        } else {
            // 5.0.0_alpha1 and later
            out.writeList(plugins);
            out.writeList(modules);
        }
    }

    /**
     * Returns an ordered list based on plugins name
     */
    public List<PluginInfo> getPluginInfos() {
        List<PluginInfo> plugins = new ArrayList<>(this.plugins);
        Collections.sort(plugins, (p1, p2) -> p1.getName().compareTo(p2.getName()));
        return plugins;
    }
    
    /**
     * Returns an ordered list based on modules name
     */
    public List<PluginInfo> getModuleInfos() {
        List<PluginInfo> modules = new ArrayList<>(this.modules);
        Collections.sort(modules, (p1, p2) -> p1.getName().compareTo(p2.getName()));
        return modules;
    }

    public void addPlugin(PluginInfo info) {
        plugins.add(info);
    }
    
    public void addModule(PluginInfo info) {
        modules.add(info);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("plugins");
        for (PluginInfo pluginInfo : getPluginInfos()) {
            pluginInfo.toXContent(builder, params);
        }
        builder.endArray();
        // TODO: not ideal, make a better api for this (e.g. with jar metadata, and so on)
        builder.startArray("modules");
        for (PluginInfo moduleInfo : getModuleInfos()) {
            moduleInfo.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }
}
