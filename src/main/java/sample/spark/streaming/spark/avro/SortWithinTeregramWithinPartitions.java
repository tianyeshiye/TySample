package sample.spark.streaming.spark.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import sample.spark.streaming.CanUnitBean;
import sample.spark.streaming.TelegramHash;
import scala.Tuple2;

public class SortWithinTeregramWithinPartitions implements
        PairFlatMapFunction<Iterator<Tuple2<TelegramHash, CanUnitBean>>, TelegramHash, List<List<CanUnitBean>>> {

    private static final long serialVersionUID = -2022345678L;

    private final int timeInterval;

    public SortWithinTeregramWithinPartitions(int timeInterval) {

        this.timeInterval = timeInterval;
    }

    public Iterator<Tuple2<TelegramHash, List<List<CanUnitBean>>>> call(
            final Iterator<Tuple2<TelegramHash, CanUnitBean>> tuples) throws Exception {

        return new Iterator<Tuple2<TelegramHash, List<List<CanUnitBean>>>>() {

            boolean repeatedHandlerFlg = true;

            private TelegramHash progress = null;

            private Tuple2<TelegramHash, CanUnitBean> aheadTuple = null;

            private TreeMap<Long, List<CanUnitBean>> canUnitBeanTreeMap;

            private void ensureNexrElement() {

                if (progress != null || canUnitBeanTreeMap != null) {
                    return;
                }

                this.canUnitBeanTreeMap = new TreeMap<Long, List<CanUnitBean>>();

                if (aheadTuple != null) {

                    this.progress = aheadTuple._1;
                    addTocanUnitBeanHashMap(canUnitBeanTreeMap, aheadTuple);
                    this.aheadTuple = null;
                }

                while (tuples.hasNext()) {

                    final Tuple2<TelegramHash, CanUnitBean> tuple = tuples.next();

                    if (progress == null || progress.equals(tuple._1)) {

                        this.progress = tuple._1;

                        addTocanUnitBeanHashMap(canUnitBeanTreeMap, tuple);

                    } else {

                        this.aheadTuple = tuple;
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {

                ensureNexrElement();

                return canUnitBeanTreeMap != null && !canUnitBeanTreeMap.isEmpty();
            }

            @Override
            public Tuple2<TelegramHash, List<List<CanUnitBean>>> next() {

                if (!hasNext()) {

                    return null;
                }

                Tuple2<TelegramHash, List<List<CanUnitBean>>> next = new Tuple2<TelegramHash, List<List<CanUnitBean>>>(
                        progress, new ArrayList<>(canUnitBeanTreeMap.values()));

                this.progress = null;
                this.canUnitBeanTreeMap = null;
                return next;
            }

            private void addTocanUnitBeanHashMap(Map<Long, List<CanUnitBean>> canUnitBeanHashMap,
                    Tuple2<TelegramHash, CanUnitBean> tuple) {

                CanUnitBean canUnitBean = tuple._2;

                Long canTime = canUnitBean.getCanTime();

                List<CanUnitBean> canUnitBeanList;

                if (canUnitBeanHashMap.containsKey(canTime)) {

                    canUnitBeanList = canUnitBeanHashMap.get(canTime);

                    //// ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
                    /// repeated Handler is needed
                    if (repeatedHandlerFlg) {

                        repeatedHandler(canUnitBeanList, tuple);

                    } else {

                        canUnitBeanList.add(canUnitBean);
                    }

                } else {

                    canUnitBeanList = new ArrayList<CanUnitBean>();

                    //// ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
                    /// repeated Handler is needed
                    if (repeatedHandlerFlg) {

                        repeatedHandler(canUnitBeanList, tuple);

                    } else {

                        canUnitBeanList.add(canUnitBean);
                    }

                    canUnitBeanHashMap.put(canTime, canUnitBeanList);
                }
            }

            ///// ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
            ///// repeated Handler

            // Map<CanUnitBean.canTime, List<CanUnitBean.canId> >
            private Map<Long, List<Short>> repeatedMap = new HashMap<Long, List<Short>>();

            private long timestamp;

            // repeated canUnitBean Handler
            private void repeatedHandler(List<CanUnitBean> canUnitBeanList, Tuple2<TelegramHash, CanUnitBean> tuple) {

                CanUnitBean newCanUnitBean = tuple._2;

                if (this.progress.timestamp != this.timestamp) {

                    repeatedMap = new HashMap<Long, List<Short>>();

                    this.timestamp = this.progress.timestamp;
                }

                List<Short> canIdlist = new ArrayList<Short>();

                if (repeatedMap.containsKey(newCanUnitBean.getCanTime())) {

                    canIdlist = repeatedMap.get(newCanUnitBean.getCanTime());

                    int index = canIdlist.indexOf(newCanUnitBean.getCanId());

                    if (index != -1) {

                        canUnitBeanList.set(index, newCanUnitBean);
                    } else {

                        canUnitBeanList.add(newCanUnitBean);

                        canIdlist.add(newCanUnitBean.getCanId());
                    }
                } else {

                    canUnitBeanList.add(newCanUnitBean);

                    canIdlist.add(newCanUnitBean.getCanId());

                    repeatedMap.put(newCanUnitBean.getCanTime(), canIdlist);
                }
            }
        };
    }

}