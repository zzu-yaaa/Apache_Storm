package storm.blueprints;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index = 0;

    /**
     * 스파우트 컴포넌트 초기화
     * @param map 스톰 설정 정보를 가짐
     * @param topologyContext 토폴로지에 속한 컴포넌트들의 정보를 가짐
     * @param collector 튜플을 내보낼 때 사용하는 메서드를 가짐
     * */
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 스파우트의 역할 정의
     * 한 문장씩 내보내도록 구현
     * */
    @Override
    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index++;
        if(index >= sentences.length){
            index = 0;
        }
        //Utils.waitForMillis(1); //교재 내용이나 현재 지원하지 않는 것으로 추정하여 대체
        Utils.sleep(1);
    }

    /**
     * 컴포넌트가 어떤 스트림을 내보내고,
     * 스트림의 튜플이 어떤 필드들로 구성되어 있는지 정의
     * */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //sentence라는 단일 필드를 가진 튜플로 구성된 한 개의 기본 스트림 내보냄
        declarer.declare(new Fields("sentence"));
    }
}
