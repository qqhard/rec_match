package match.swing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

@Description(
        name = "collect",
        value = "_FUNC_(col) - The parameter is a column name. "
                + "The return value is a set of the column.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) from src;"
)
public class SwingUDAF extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(SwingUDAF.class.getName());

    public SwingUDAF() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 2) {
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

        private ListObjectInspector inputItemsOI;
        private PrimitiveObjectInspector inputUvOI;

        // input For merge()
        StructObjectInspector soi;
        StructField uvField;
        StructField itemsField;
        LongObjectInspector uvFieldOI;
        StandardListObjectInspector itemsFieldOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            LOG.error(m.toString() + ":" + m.name() + ":" + parameters[0].getClass());

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputItemsOI = (ListObjectInspector) parameters[0];
                inputUvOI = (PrimitiveObjectInspector) parameters[1];
                /*
                 * 构造Struct的OI实例，用于设定聚合结果数组的类型
                 * 需要字段名List和字段类型List作为参数来构造
                 */
            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                soi = (StructObjectInspector) parameters[0];
                uvField = soi.getStructFieldRef("uv");
                itemsField = soi.getStructFieldRef("items");
                //数组中的每个数据，需要其各自的基本类型OI实例解析
                uvFieldOI = (LongObjectInspector) uvField.getFieldObjectInspector();
                itemsFieldOI = (StandardListObjectInspector) itemsField.getFieldObjectInspector();
                inputItemsOI = (StandardListObjectInspector) itemsFieldOI.getListElementObjectInspector();
                LOG.error(uvFieldOI);
                LOG.error(itemsFieldOI);
            }

            // output
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("items");
                fname.add("uv");
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.javaStringObjectInspector
                        )
                ));
                foi.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.STRING);
            }
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {
            List<List<Object>> container;
            long uv;
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
            if (param.length == 2) {
                ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
                Object pCopy = ObjectInspectorUtils.copyToStandardObject(param[0], this.inputItemsOI);
                myAgg.container.add((List<Object>) pCopy);
                myAgg.uv = PrimitiveObjectInspectorUtils.getLong(param[1], this.inputUvOI);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;

            Object partialUv = soi.getStructFieldData(partial, uvField);
            Object partialItems = soi.getStructFieldData(partial, itemsField);
            //通过基本数据类型的OI实例解析Object的值

            myAgg.uv = uvFieldOI.get(partialUv);

            List<Object> list = (List<Object>) itemsFieldOI.getList(partialItems);

            for (Object param : list) {
                Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, inputItemsOI);
                myAgg.container.add((List<Object>) pCopy);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<List<Object>> list = new ArrayList<List<Object>>();
            list.addAll(myAgg.container);
            return new Object[]{list, myAgg.uv};
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            List<Set<ItemUvNode>> buffer = new ArrayList<Set<ItemUvNode>>();
            for (Object o : myAgg.container) {
                List<Text> itemNeighborsOri = (List<Text>) o;
                Set<ItemUvNode> itemNeighbors = new HashSet<>();
                for (Text txt : itemNeighborsOri) {
                    String str = txt.toString();
                    ItemUvNode node = new ItemUvNode();
                    node.itemId = str.split(":")[0];
                    node.uv = Long.parseLong(str.split(":")[1]);
                    itemNeighbors.add(node);
                }
                buffer.add(itemNeighbors);
            }

            List<ItemScoreNode> result = itemCF(buffer, myAgg.uv);
            if (!result.isEmpty()){
                DecimalFormat df = new DecimalFormat("0.00000");
                df.setRoundingMode(RoundingMode.HALF_UP);
                StringBuilder sb = new StringBuilder();
                for (int index = result.size() - 1; index >= 0; index--) {
                    sb.append(",")
                            .append(result.get(index).itemId)
                            .append(":")
                            .append(df.format(result.get(index).score));
                }
                return sb.substring(1).toString();
            }
            return "";
        }

        static class ItemUvNode {
            String itemId;
            long uv;

            @Override
            public int hashCode() {
                return itemId.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                if (obj instanceof ItemUvNode && itemId != null) {
                    return itemId.equals(((ItemUvNode) obj).itemId);
                }
                return false;
            }

            @Override
            public String toString() {
                return itemId + ":" + uv;
            }
        }

        static class ItemScoreNode implements Comparable {
            String itemId;
            double score;


            @Override
            public int compareTo(Object o) {
                return Double.compare(score, ((ItemScoreNode) o).score);
            }

            @Override
            public String toString() {
                return itemId + ":" + score;
            }
        }

        private List<ItemScoreNode> itemCF(List<Set<ItemUvNode>> buffer, long uv) {
            int topK = 200;
            Map<String, Double> statMap = new HashMap<String, Double>();
            Set<ItemUvNode> tmpSet = new HashSet<>();
            for (int indexI = 0; indexI < buffer.size(); indexI++) {
                double weightI = Math.pow(buffer.get(indexI).size() + 5, -0.3);
                for (int indexJ = indexI + 1; indexJ < buffer.size(); indexJ++) {
                    double weightJ = Math.pow(buffer.get(indexJ).size() + 5, -0.3);
                    tmpSet.clear();
                    tmpSet.addAll(buffer.get(indexI));
                    tmpSet.retainAll(buffer.get(indexJ));
                    for (ItemUvNode node : tmpSet) {
                        statMap.put(node.itemId,
                                weightI * weightJ / (1.0 + tmpSet.size()) +
                                        statMap.getOrDefault(node.itemId, 0.0));
                    }
                }
            }

            PriorityQueue<ItemScoreNode> queue = new PriorityQueue<ItemScoreNode>(topK);
            for (Map.Entry<String, Double> entry : statMap.entrySet()) {
                if (queue.size() < topK) {
                    ItemScoreNode isNode = new ItemScoreNode();
                    isNode.itemId = entry.getKey();
                    isNode.score = entry.getValue();
                    queue.add(isNode);
                } else {
                    if (entry.getValue() > queue.peek().score) {
                        ItemScoreNode isNode = new ItemScoreNode();
                        isNode.itemId = entry.getKey();
                        isNode.score = entry.getValue();
                        queue.remove();
                        queue.add(isNode);
                    }
                }
            }
            List<ItemScoreNode> result = new ArrayList<>();
            while (!queue.isEmpty()) {
                result.add(queue.remove());
            }
            return result;
//            LOG.error(uv + "#" + buffer + "#" + result);
        }
    }
}

