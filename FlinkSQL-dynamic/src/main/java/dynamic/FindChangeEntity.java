package dynamic;

import org.apache.flink.streaming.api.graph.StreamGraph;
import scala.tools.nsc.NewLinePrintWriter;

public interface FindChangeEntity {
    public boolean isChanged();
    public StreamGraph returnStreamGraph();
}
