package storm.blueprints;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception{
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();

        //스파우트 및 볼트 추가 -> 객체에 고유 ID 할당
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        //grouping 통해 어떤 방식으로 데이터를 받을지 지정
        builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();

        System.out.println("=====[submit]====================================");
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        //waitForSeconds(10);
        System.out.println("=====[sleep]====================================");
        Thread.sleep(10000);
        System.out.println("=====[kill]====================================");
        cluster.killTopology(TOPOLOGY_NAME);
        System.out.println("=====[shutdown]====================================");
        cluster.shutdown();
    }
}
