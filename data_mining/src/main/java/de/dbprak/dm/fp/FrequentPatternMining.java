package de.dbprak.dm.fp;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.util.List;

/**
 * @author fdecker
 */
public class FrequentPatternMining {

    public static void analyze(JavaSparkContext sc, IFPCollector collector, double minSupport, double minConfidence, int partitions, int minItemSets){
        List<List<String>> receiptItemList = collector.getData();

        long endTimeStringToList = System.currentTimeMillis();

        JavaRDD<List<String>> transactions = sc.parallelize(receiptItemList);
        long endTimeToRDD = System.currentTimeMillis();
        System.out.println("Time for endTimeToRDD:"+(endTimeToRDD-endTimeStringToList)+"ms");

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupport)
                .setNumPartitions(partitions);
        FPGrowthModel<String> model = fpg.run(transactions);

        long endTimeFPGrowth = System.currentTimeMillis();
        System.out.println("Time for endTimeFPGrowth:"+(endTimeFPGrowth-endTimeToRDD)+"ms");

        for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
            if(itemset.javaItems().size()>=minItemSets) {
                System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
            }
        }

        for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

    }
}