package stormit;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public abstract class BoltFilter implements Serializable{
    private StreamItBolt streamItBolt;

    private LinkedList<Tuple> tuples;

    abstract void execute();

    void invoke(LinkedList<Tuple> tuples){
        this.tuples = tuples;
        execute();
    }

    void push(List<Object> values){
        streamItBolt.emit(values); // TODO: Fix this properly. Check anchor.
    }

    void pop(){
        streamItBolt.pop();
    }

    Tuple peek(int i){
        return tuples.get(i);
    }

    void setStreamItBolt(StreamItBolt streamItBolt){
        this.streamItBolt = streamItBolt;
    }
}
