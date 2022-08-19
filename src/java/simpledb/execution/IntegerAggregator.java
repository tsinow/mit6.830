package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op what;
    HashMap<Field, List<Integer>> map;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        this.what = what;
        map = new HashMap<>();
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbIndex = null;
        if (gbField != NO_GROUPING)
            gbIndex = tup.getField(gbField);
        int aValue = tup.getField(aField).hashCode();

        if (map.containsKey(gbIndex)) {
            List<Integer> oldList = map.get(gbIndex);
            oldList.add(aValue);
            map.replace(gbIndex, oldList);
        } else {
            ArrayList<Integer> list = new ArrayList<>();
            list.add(aValue);
            map.put(gbIndex, list);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc;
        if (gbField == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
        }
        List<Tuple> tupleList = new ArrayList<>();
        for (Map.Entry<Field, List<Integer>> entry : map.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            Field index = entry.getKey();
            List<Integer> list = entry.getValue();
            int aValue = 0;
            for (int i = 0; i < list.size(); i++) {

                switch (what) {
                    case COUNT:
                        aValue++;
                        break;
                    case AVG:
                    case SUM:
                        aValue += list.get(i);
                    case MAX:
                        if (aValue < list.get(i))
                            aValue = list.get(i);
                        break;
                    case MIN:
                        if (i == 0) aValue = list.get(0);
                        if (aValue > list.get(i)) aValue = list.get(i);
                        break;
                }
            }
            if (what == Op.AVG) {
                aValue /= list.size();
            }

            if (gbField != NO_GROUPING) {
                tuple.setField(0, index);
                tuple.setField(1, new IntField(aValue));
            } else {
                tuple.setField(0, new IntField(aValue));
            }

            tupleList.add(tuple);
        }
        return new TupleIterator(tupleDesc, tupleList);

    }

}
