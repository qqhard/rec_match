package match.i2i;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

@Description(
        name = "collect",
        value = "_FUNC_(col) - The parameter is a column name. "
                + "The return value is a set of the column.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) from src;"
)
public class I2IUDAF extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(I2IUDAF.class.getName());

    public I2IUDAF() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    "Only list type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        return new GenericUDAFCollectEvaluator();
    }

    @SuppressWarnings("deprecation")
    public static class GenericUDAFCollectEvaluator extends GenericUDAFEvaluator {

        private ListObjectInspector inputOI;
        private StandardListObjectInspector internalMergeOI;
        private StandardListObjectInspector loi;
        private PrimitiveObjectInspector foi;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            LOG.error(m.toString() + ":" + m.name() + ":" + parameters[0].getClass());

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (ListObjectInspector) parameters[0];
                loi = ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorUtils
                                .getStandardObjectInspector(inputOI));
                return loi;
            } else if (m == Mode.PARTIAL2) {
                internalMergeOI = (StandardListObjectInspector) parameters[0];
                LOG.error(internalMergeOI.getListElementObjectInspector().getClass());
                inputOI = (ListObjectInspector) internalMergeOI.getListElementObjectInspector();
                loi = ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
                return loi;
            } else if (m == Mode.FINAL) {
                internalMergeOI = (StandardListObjectInspector) parameters[0];
                LOG.error(internalMergeOI.getListElementObjectInspector().getClass());
                inputOI = (ListObjectInspector) internalMergeOI.getListElementObjectInspector();
                foi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                                PrimitiveObjectInspector.PrimitiveCategory.INT);

                return foi;
            }
            return null;
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {
            List<List<Object>> container;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).container = new ArrayList<List<Object>>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] param)
                throws HiveException {
            Object p = param[0];
            if (p != null) {
                putIntoList(p, (ArrayAggregationBuffer) agg);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> partialResult = (ArrayList<Object>) this.internalMergeOI.getList(partial);
            for (Object obj : partialResult) {
                putIntoList(obj, myAgg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> list = new ArrayList<Object>();
            list.addAll(myAgg.container);
            LOG.error(list);
            int cnt = 0;
            for (Object o : myAgg.container) {
                List oo = (List)o;
                cnt += oo.size();
            }
            return cnt+300;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<List<Object>> list = new ArrayList<List<Object>>();
            list.addAll(myAgg.container);
            LOG.error(list);
            return list;
        }

        public void putIntoList(Object param, ArrayAggregationBuffer myAgg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, this.inputOI);
            myAgg.container.add((List<Object>) pCopy);
        }
    }
}

