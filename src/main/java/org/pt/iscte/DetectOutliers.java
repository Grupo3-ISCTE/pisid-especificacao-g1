package org.pt.iscte;

import java.lang.reflect.Array;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;


public class DetectOutliers {

    public static void main(String[] args) {

//        List<Integer> values = new ArrayList<>();
//        values.add(100);
//        values.add(105);
//        values.add(102);
//        values.add(13);
//        values.add(104);
//        values.add(22);
//        values.add(101);
//
//        System.out.println("Before: " + values);
//        System.out.println("After: " + eliminateOutliers(values, 1.5f));

//        ArrayList<Integer> t = new ArrayList<Integer>();
//        ArrayList<Integer> t2 = new ArrayList<Integer>();
//
//        for (int i = 0; i < 10; i++)
//            t.add(i);
//
//        for (int i = 0; i < 10; i++)
//            t2.add(i);
//
//        t.addAll(t2);
//        System.out.println(t);
        String s="M 1";
        System.out.println(s.charAt(0));
    }

    public DetectOutliers() {
    }

    protected double getMean(ArrayList<Medicao> values) {
        int sum = 0;
        for (Medicao value : values) {
            sum += value.getLeitura();
        }

        return (sum / values.size());
    }

    public double getVariance(ArrayList<Medicao> values) {
        double mean = getMean(values);
        int temp = 0;

        for (Medicao m : values) {
            temp += (m.getLeitura() - mean) * (m.getLeitura() - mean);
        }

        return temp / (values.size() - 1);
    }

    public double getStdDev(ArrayList<Medicao> values) {
        return Math.sqrt(getVariance(values));
    }

    public ArrayList<Medicao> eliminateOutliers(ArrayList<Medicao> medicoes, float scaleOfElimination) {
        double mean = getMean(medicoes);
        double stdDev = getStdDev(medicoes);

        final ArrayList<Medicao> newList = new ArrayList<>();

        for (Medicao medicao : medicoes) {
            boolean isLessThanLowerBound = medicao.getLeitura() < mean - stdDev * scaleOfElimination;
            boolean isGreaterThanUpperBound = medicao.getLeitura() > mean + stdDev * scaleOfElimination;
            boolean isOutOfBounds = isLessThanLowerBound || isGreaterThanUpperBound;

            if (!isOutOfBounds) {
                newList.add(medicao);
            }
        }

        int countOfOutliers = medicoes.size() - newList.size();
        if (countOfOutliers == 0) {
            return medicoes;
        }

        return eliminateOutliers(newList, scaleOfElimination);
    }
}
