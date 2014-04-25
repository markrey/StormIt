package stormit;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public abstract class BoltFilter implements Serializable{
    private StreamItBolt streamItBolt;

    private LinkedList<Tuple> tuples;

    public abstract void execute();

    public void invoke(LinkedList<Tuple> tuples){
        this.tuples = tuples;
        execute();
    }

    public void push(List<Object> values){
        streamItBolt.emit(values); // TODO: Fix this properly. Check anchor.
    }

    public Tuple pop(){
        return streamItBolt.pop();
    }

    public Tuple peek(int i){
        return tuples.get(i);
    }

    public void setStreamItBolt(StreamItBolt streamItBolt){
        this.streamItBolt = streamItBolt;
    }
}
