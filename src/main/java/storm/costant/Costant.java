package storm.costant;

public class Costant {
    public static final int TOP_K = 10;
    public static final String INTERMEDIATERANK_15M = "intermediate15M" ;
    public static final String INTERMEDIATERANK_1H = "intermediate1H" ;
    public static final String INTERMEDIATERANK_24H = "intermediate24H" ;


    public static final String F_RECORD = "record";
    public static final String F_START_PROCESSING_TIME = "time";

    public static final String GLOBAL15M_AVG = "globalAvg15M";
    public static final String GLOBAL1H_AVG = "globalAvg1H";
    public static final String GLOBAL24H_AVG = "globalAvg24H";

    public static final String KAFKA_SPOUT = "kafka_spout";

    public static final String PARTIAL_RANK = "partialRank";
    public static final String RANK_TOPK = "rankTopK";
    public static final String FILTER_CONTROL = "filterControl" ;
    public static final int NUM_FILTER_CONTROL = 4;
    public static final int NUM_SPOUT_CONTROL = 1;
    public static final String SUM_BOLT = "sumBolt";
    public static final int NUM_SUM_BOLT = 4;
    public static final String WEBSTER_BOLT = "websterBolt";
    public static final int NUM_WEBSTER_BOLT = 3;
    public static final String MAP_INTERSECTION = "mapIntersection";
    public static final String PHASE = "phase";
    public static final String MONGODB15M = "mongodb15M";
    public static final String MONGODB1H = "mongodb1h";
    public static final String MONGODB24H = "mongodb24h";
    public static final int NUM_MONGOBOLT15M = 1;
    public static final int NUM_MONGOBOLT1H = 1;
    public static final int NUM_MONGOBOLT24H = 1;
    public static final String QUERY1 = "query1";

    public static int N_INTERSECTIONS = 50;
    public static int YELLOW_TIME = 4;
    public static int CYCLE_TIME = 200;
    public static int CHANGE_TIME = 5;  //i
    public static int LOST_TIME= 3;
    public static int ALL_RED = 1;
    public static int THRESOLD_SATURATION = 7000;
    public static int RANGE_SATURATION = 500;
    public static int NUM_PHASE = 2;


    public static final int SEM_INTERSEC = 4;
    // QUERY 1 Costants
    public static final int WINDOW_MIN = 15;
    public static final int WINDOW_HOUR = 1;
    public static final int WINDOW_DAY = 1;

    public static final int NUM_AVG15M = 3;
    public static final int NUM_AVG1H = 3;
    public static final int NUM_AVG24H = 3;

    // QUERY 2 Costants
    public static final int NUM_MEDIAN_15M_BOLT = 1;
    public static final int NUM_MEDIAN_1H_BOLT  = 2;
    public static final int NUM_MEDIAN_24H_BOLT = 4;

    public static final int NUM_INTERMEDIATERANK15M = 5;
    public static final int NUM_INTERMEDIATERANK1H = 5;
    public static final int NUM_INTERMEDIATERANK24H = 5;

    public static final int NUM_FILTER_QUERY1 = 1;
    public static final int NUM_FILTER_QUERY2 = 1;
    public static final int SEC_TUPLE = 60;
    public static final int COMPRESSION = 100;
    public static final double QUANTIL = 0.5;
    public static final int NUM_SPOUT_QUERY_1=1;
    public static final int NUM_SPOUT_QUERY_2=1;

    public static final String TOPOLOGY_QUERY_1="topologiaClassifica";
    public static final String TOPOLOGY_QUERY_2="topologiaMediana";
    public static final String SPOUT ="semaphore";
    public static final String FILTER_QUERY_1 = "filterBolt";
    public static final String FILTER_QUERY_2="filterBoltMed";
    public static final String AVG15M_BOLT = "Avg15MBolt";
    public static final String AVG1H_BOLT = "Avg1HBolt";
    public static final String AVG24H_BOLT = "Avg24HBolt";

    public static final String MEDIAN15M_BOLT="Median15MBolt";
    public static final String MEDIAN1H_BOLT="Median1HBolt";
    public static final String MEDIAN24H_BOLT="Median24HBolt";
    public static final String ID="id";
    public static final String GLOBAL15M_MEDIAN="global15MMed";
    public static final String GLOBAL1H_MEDIAN="global1HMed";
    public static final String GLOBAL24H_MEDIAN="global24HMed";
    public static final String SENSOR="sensore";
    public static final String INTERSECTION="incrocio";
    public static final String LIST_INTERSECTION="listaincroci";

    public static final int NUM_GLOBAL_BOLT=1;
    public static final String MEDIAN="median";
    public static final int MESSAGE_TIMEOUT_SEC=1900000;
    public static final String RESULT = "result";
    public static final int NUM_PARALLELISM = 20;

    public static final String ID15M = "15M";
    public static final String ID1H = "1H";
    public static final String ID24H = "24H";
    public static int TIME_ERROR = 1;
}
