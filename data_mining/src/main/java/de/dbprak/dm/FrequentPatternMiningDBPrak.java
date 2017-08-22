package de.dbprak.dm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.List;

/**
 * @author fdecker
 */
public class FrequentPatternMiningDBPrak {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaAssociationRulesExample").setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //IFPCollector ifpCollector1 = new MongoReceiptCollector();
        IFPCollector ifpCollector2 = new SqlReceiptCollector();

        //analyze(sc, ifpCollector1);
        analyze(sc, ifpCollector2);
    }

    public static void analyze(JavaSparkContext sc, IFPCollector collector){
        List<List<String>> receiptItemList = collector.getData();

        long endTimeStringToList = System.currentTimeMillis();

        JavaRDD<List<String>> transactions = sc.parallelize(receiptItemList);
        long endTimeToRDD = System.currentTimeMillis();
        System.out.println("Time for endTimeToRDD:"+(endTimeToRDD-endTimeStringToList)+"ms");

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.01)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);

        long endTimeFPGrowth = System.currentTimeMillis();
        System.out.println("Time for endTimeFPGrowth:"+(endTimeFPGrowth-endTimeToRDD)+"ms");

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        double minConfidence = 0.8;
        for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

    }
}