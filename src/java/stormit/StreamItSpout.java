/**
 * Licensed to Milinda Pathirage under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stormit;

import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import clojure.lang.IFn;
import clojure.lang.RT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamItSpout implements IRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(StreamItSpout.class);

    private Map stormConf;

    private SpoutOutputCollector collector;

    private TopologyContext context;

    private long pushCount;

    Map<String, StreamInfo> outputSpec;

    List<String> prepareFnSpec;

    List<Object> params;

    SpoutFilter spoutFilter;

    public StreamItSpout(List<String> prepareFnSpec,
                         List<Object> params,
                         Map<String, StreamInfo> outputSpec,
                         long pushCount){
        this.pushCount = pushCount;
        this.prepareFnSpec = prepareFnSpec;
        this.params = params;
        this.outputSpec = outputSpec;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String stream: outputSpec.keySet()) {
            StreamInfo info = outputSpec.get(stream);
            declarer.declareStream(stream, info.is_direct(), new Fields(info.get_output_fields()));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.stormConf = conf;
        this.context = context;
        this.collector = collector;

        // Prepare boltFilter from clojure
        IFn hof = Utils.loadClojureFn(prepareFnSpec.get(0), prepareFnSpec.get(1));
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(params));

            spoutFilter = (SpoutFilter) preparer.applyTo(RT.seq(Collections.emptyList()));
            spoutFilter.setStreamItSpout(this);
        } catch (Exception e) {
            LOG.error("Something went wrong during initialization of SpoutFilter.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        spoutFilter.nextTuple();
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    public void emit(List<Object> values) {
        collector.emit(values); // TODO: Check this.
    }

}
