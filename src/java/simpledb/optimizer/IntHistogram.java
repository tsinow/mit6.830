package simpledb.optimizer;

import simpledb.common.DbException;
import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int bucketNum;
    private final int min;
    private final int max;
    private final double width;
    private int tupNum;
    private final HashMap<Integer, Integer>[] buckets;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.bucketNum = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + 0.0) / bucketNum;
        this.buckets = new HashMap[bucketNum];
        for (int i = 0; i < bucketNum; i++) {
            this.buckets[i] = new HashMap<>();
        }
        tupNum = 0;
    }

    //   index start from 0
    private int bucketIndex(int v) {
        int result;
        result = (int) ((v - min + 0.0) / width);
        if (v == max) result--;
        return result;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */

    public void addValue(int v) {
        // some code goes here
        if (v < min || v > max) try {
            throw new DbException("the value is out of bucket range");
        } catch (DbException e) {
            e.printStackTrace();
        }
        buckets[bucketIndex(v)].merge(v, 1, Integer::sum);
        tupNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        double selectNum = 0.0;
        int bucketIndex = bucketIndex(v);
        HashMap<Integer, Integer> targetHashMap;
        if (v >= min && v <= max)
            targetHashMap = buckets[bucketIndex];
        else
            targetHashMap = null;
        switch (op) {
            case EQUALS:
                if (v > max || v < min)
                    return 0.0;
            case NOT_EQUALS:
                if (v > max || v < min)
                    return 1.0;
                // System.out.println("min is " + min + " max is " + max + "v is " + v);

                if (targetHashMap.containsKey(v)) selectNum = targetHashMap.get(v);

                if (op == Predicate.Op.NOT_EQUALS)
                    selectNum = tupNum - selectNum;
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                if (v > max) return 1.0;
                if (v < min) return 0.0;
                for (int i = 0; i < bucketIndex; i++) {
                    HashMap<Integer, Integer> hashMap = buckets[i];
                    for (int j : hashMap.values()) {
                        selectNum += j;
                    }
                }
                int sum = 0;
                for (int i : targetHashMap.values()) sum += i;

                selectNum += ((v - min) / width) * ((double) sum / tupNum);

                if (op == Predicate.Op.LESS_THAN_OR_EQ) {
                    if (targetHashMap.containsKey(v)) selectNum += targetHashMap.get(v);
                }
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                if (v > max) return 0.0;
                if (v < min) return 1.0;

                for (int i = bucketNum-1; i > bucketIndex; i--) {
                    HashMap<Integer, Integer> hashMap = buckets[i];
                    for (int j : hashMap.values()) {
                        selectNum += j;
                    }
                }
                sum = 0;
                for (int i : targetHashMap.values()) sum += i;
                selectNum += ((max - v) / width) * ((double) sum / tupNum);

                if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
                    if (targetHashMap.containsKey(v)) selectNum += targetHashMap.get(v);
                }
                break;
        }
        return selectNum / tupNum;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "the IntHistogram has " + bucketNum + " buckets and value ranges from " + min + " to " + max + " .";
    }
}
