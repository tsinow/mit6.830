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
    private final HashMap<Field, Integer> gbGroupAggregatedValues; // <GroupBy Column's Value, Aggregated Value>
    private final HashMap<Field, Integer> gbGroupNTuple;

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
        gbGroupAggregatedValues = new HashMap<>();
        gbGroupNTuple = new HashMap<>();
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        resultIterator = null;

        Field gbKey = null;
        if (gbField != NO_GROUPING)
            gbKey = tup.getField(gbField);
        IntField f = (IntField)tup.getField(aField);

        gbGroupNTuple.put(gbKey, gbGroupNTuple.getOrDefault(gbKey, 0)+1);

        Integer aValue = gbGroupAggregatedValues.get(gbKey);

        switch (what) {
            case COUNT:
                if(aValue == null) aValue = 0;
                aValue = aValue + 1;
                break;
            case AVG:
            case SUM:
                if(aValue == null) aValue = 0;
                aValue = aValue + f.getValue();
                break;
            case MAX:
                if(aValue == null) aValue = f.getValue();
                aValue = Math.max(aValue, f.getValue());
                break;
            case MIN:
                if(aValue == null) aValue = f.getValue();
                aValue = Math.min(aValue, f.getValue());
                break;
            default:
                throw new RuntimeException("invalid aggregate operation, are you dumb?");
        }
        gbGroupAggregatedValues.put(gbKey, aValue);
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
        if(resultIterator == null) {
            resultIterator = buildIterator();
        }
        return resultIterator;
    }

    private OpIterator resultIterator;

    private OpIterator buildIterator() {
        // some code goes here
        TupleDesc tupleDesc;
        if (gbField == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
        }
        List<Tuple> tupleList = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : gbGroupAggregatedValues.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            Field gbKey = entry.getKey();
            Integer aValue = entry.getValue();

            if (what == Op.AVG) {
                aValue /= gbGroupNTuple.get(gbKey);
            }

            if (gbField != NO_GROUPING) {
                tuple.setField(0, gbKey);
                tuple.setField(1, new IntField(aValue));
            } else {
                tuple.setField(0, new IntField(aValue));
            }
            tupleList.add(tuple);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }

}
