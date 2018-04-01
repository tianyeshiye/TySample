package sample.spark.streaming;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class ChangeDataStreamingFunction implements
        PairFlatMapFunction<Iterator<Tuple2<TelegramHash, List<CanUnitBean>>>, TelegramHash, List<CanUnitBean>> {

    public Iterator<Tuple2<TelegramHash, List<CanUnitBean>>> call(
            final Iterator<Tuple2<TelegramHash, List<CanUnitBean>>> tuples) throws Exception {

        return new Iterator<Tuple2<TelegramHash, List<CanUnitBean>>>() {

            private List<CanUnitBean> changedCanUnitBeanList;

            private TelegramHash progress;

            private HashMap<Long, CanUnitBean> subCanHashMap = new HashMap<Long, CanUnitBean>();

            private List<Long> longListOf0E = new ArrayList<Long>();

            private void ensureNexrElement() {

                if (changedCanUnitBeanList != null && !changedCanUnitBeanList.isEmpty()) {
                    return;
                }

                while (tuples.hasNext()) {

                    final Tuple2<TelegramHash, List<CanUnitBean>> tuple = tuples.next();

                    this.progress = tuple._1;

                    this.changedCanUnitBeanList = tuple._2;

                    convertDataOfCommonCan(changedCanUnitBeanList);

                    break;
                }
            }

            @Override
            public boolean hasNext() {

                ensureNexrElement();

                return changedCanUnitBeanList != null && !changedCanUnitBeanList.isEmpty();
            }

            @Override
            public Tuple2<TelegramHash, List<CanUnitBean>> next() {

                if (!hasNext()) {

                    return null;
                }

                convertDataOf0X22(changedCanUnitBeanList);

                Tuple2<TelegramHash, List<CanUnitBean>> next = new Tuple2<TelegramHash, List<CanUnitBean>>(progress,
                        changedCanUnitBeanList);

                this.changedCanUnitBeanList = null;
                this.progress = null;
                
                this.subCanHashMap = new HashMap<Long, CanUnitBean>();
                this.longListOf0E = new ArrayList<Long>();

                return next;
            }

            // convert 0x22 以外
            private void convertDataOfCommonCan(List<CanUnitBean> canUnitBeanList) {

                for (CanUnitBean canUnitBean : canUnitBeanList) {

                    if (canUnitBean.getCanId() == 0x0E) {

                        subCanHashMap.put(canUnitBean.getCanTime(), canUnitBean);

                        longListOf0E.add(canUnitBean.getCanTime());
                    }

                    if (canUnitBean.getCanId() != 0x22) {

                        // convert 0x22 以外
                        Unit.converedCanUnitBean(canUnitBean, null);
                    }
                }
            }

            // convert 0x22
            private void convertDataOf0X22(List<CanUnitBean> canUnitBeanList) {

                Collections.sort(longListOf0E);

                for (CanUnitBean canUnitBean : canUnitBeanList) {

                    if (canUnitBean.getCanId() == 0x22) {

                        long canTime = canUnitBean.getCanTime();

                        long longOf0E = getNearByCanTime(longListOf0E, canTime);

                        // convert 0x22
                        Unit.converedCanUnitBeanOf0X22(canUnitBean, subCanHashMap.get(longOf0E));
                    }
                }
            }

        };
    }

    private Long getNearByCanTime(List<Long> list, Long value) {

        int size = list.size();

        if (size == 0) {

            return 0L;
        }
        if (size == 1) {

            return list.get(0);
        }

        if (list.get(size - 1) <= value) {
            return list.get(size - 1);
        }

        if (list.get(0) >= value) {
            return list.get(0);
        }

        Long returnLong = 0L;
        Long midValue;
        Long midBeforeValue;
        Long midAfterValue;

        int low = 0;
        int high = size - 1;

        while (low <= high) {

            int midIndex = (low + high) / 2;
            int beforeIndex = (midIndex - 1) > low ? (midIndex - 1) : low;
            int afterIndex = (midIndex + 1) < high ? (midIndex + 1) : high;

            midBeforeValue = list.get(beforeIndex);
            midValue = list.get(midIndex);
            midAfterValue = list.get(afterIndex);

            if (midBeforeValue <= value && value <= midValue) {

                if ((value - midBeforeValue) >= (midValue - value)) {

                    returnLong = midValue;
                } else {

                    returnLong = midBeforeValue;
                }

                break;

            } else if (midValue <= value && value <= midAfterValue) {

                if ((value - midValue) >= (midAfterValue - value)) {

                    returnLong = midAfterValue;
                } else {

                    returnLong = midValue;
                }

                break;
            } else if (midValue < value) {

                low = (low + high) / 2 + 1;

            } else if (midValue > value) {

                high = (low + high) / 2 - 1;
            }
        }

        return returnLong;
    }
}
