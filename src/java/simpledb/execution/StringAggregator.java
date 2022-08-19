package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final Type gbFieldType;
    HashMap<Field, Integer> map;

    /**
     * Aggregate constructor
     *
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("only count allow but it is" + what.toString());
        }
        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbIndex = tup.getField(gbField);

        if (map.containsKey(gbIndex)) {
            int oldValue = map.get(gbIndex);
            map.replace(gbIndex, oldValue + 1);
        } else {
            map.put(gbIndex, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
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
        for (Map.Entry<Field, Integer> entry : map.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);

            if (gbField != NO_GROUPING) {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }else{
                tuple.setField(0, new IntField(entry.getValue()));
            }

            tupleList.add(tuple);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }

}
