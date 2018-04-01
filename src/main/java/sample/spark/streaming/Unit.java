package sample.spark.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Unit {
    public static CanUnitBean createCanUnitBean(short id, String canTime) {
        CanUnitBean bean = new CanUnitBean();
        bean.setCanId(id);
        bean.setCanTime(Long.parseLong(canTime));
        Map<String, Object> values = new HashMap<>();

        switch (id) {
            case 0x0E:
                values.put("c00E_ido", 1.0F);
                values.put("c00E_keido", 1.0F);
                break;
            case 0x00:
                values.put("c000_ido", 1.0F);
                values.put("c000_keido", 1.0F);
                break;
            case 0x01:
                values.put("c001_dummy15", 1L);
                values.put("c001_dummy16", 2L);
                values.put("c001_dummy17", 3L);
                break;
            case 0x22:
                values.put("c022_dummy15", 1L);
                values.put("c022_dummy16", 2L);
                values.put("c022_dummy17", 3L);
                values.put("c022_dummy18", 4L);
                break;
            case 0x122:
                values.put("c122_dummy15", 1L);
                values.put("c122_dummy16", 2L);
                values.put("c122_dummy17", 3L);
                values.put("c122_dummy18", 4L);
                values.put("c122_dummy19", 5L);
                break;
            case 0x201:
                values.put("c201_dummy15", 1L);
                values.put("c201_dummy16", 2L);
                values.put("c201_dummy17", 3L);
                values.put("c201_dummy18", 4L);
                values.put("c201_dummy19", 5L);
                values.put("c201_dummy20", 6L);
                break;
        }
        bean.setConvertedDataMap(values);
        return bean;
    }

    public static CanUnitBean createCanUnitBeanNull(short id, String canTime) {
        CanUnitBean bean = new CanUnitBean();
        bean.setCanId(id);
        bean.setCanTime(Long.parseLong(canTime));
        Map<String, Object> values = new HashMap<>();

        switch (id) {
            case 0x0E:
                values.put("c00E_ido", 1.0F);
                values.put("c00E_keido", 1.0F);
                break;
            case 0x00:
                values.put("c000_ido", 1.0F);
                values.put("c000_keido", 1.0F);
                break;
            case 0x01:
                values.put("c001_dummy15", 111L);
                values.put("c001_dummy16", null);
                values.put("c001_dummy17", 333L);
                break;
            case 0x22:
                values.put("c022_dummy15", 111L);
                values.put("c022_dummy16", null);
                values.put("c022_dummy17", null);
                values.put("c022_dummy18", 444L);
                break;
            case 0x122:
                values.put("c122_dummy15", 111L);
                values.put("c122_dummy16", null);
                values.put("c122_dummy17", null);
                values.put("c122_dummy18", null);
                values.put("c122_dummy19", 555L);
                break;
            case 0x201:
                values.put("c201_dummy15", 111L);
                values.put("c201_dummy16", 222L);
                values.put("c201_dummy17", null);
                values.put("c201_dummy18", null);
                values.put("c201_dummy19", null);
                values.put("c201_dummy20", null);
                break;
        }
        bean.setConvertedDataMap(values);
        return bean;
    }

    public static CanUnitBean createCanUnitBean2(short id, String canTime) {
        CanUnitBean bean = new CanUnitBean();
        bean.setCanId(id);
        bean.setCanTime(Long.parseLong(canTime));
        Map<String, Object> values = new HashMap<>();

        switch (id) {
            case 0x0E:
                values.put("c00E_ido", 1.0F);
                values.put("c00E_keido", 1.0F);
                break;
            case 0x00:
                values.put("c000_ido", 1.0F);
                values.put("c000_keido", 1.0F);
                break;
            case 0x01:
                values.put("c001_dummy15", 11L);
                values.put("c001_dummy16", 22L);
                values.put("c001_dummy17", 33L);
                break;
            case 0x22:
                values.put("c022_dummy15", 11L);
                values.put("c022_dummy16", 22L);
                values.put("c022_dummy17", 33L);
                values.put("c022_dummy18", 44L);
                break;
            case 0x122:
                values.put("c122_dummy15", 11L);
                values.put("c122_dummy16", 22L);
                values.put("c122_dummy17", 33L);
                values.put("c122_dummy18", 44L);
                values.put("c122_dummy19", 55L);
                break;
            case 0x201:
                values.put("c201_dummy15", 11L);
                values.put("c201_dummy16", 22L);
                values.put("c201_dummy17", 33L);
                values.put("c201_dummy18", 44L);
                values.put("c201_dummy19", 55L);
                values.put("c201_dummy20", 66L);
                break;
        }
        bean.setConvertedDataMap(values);
        return bean;
    }

    public static CanUnitBean createRadomCanUnitBean(short id, String canTime) {
        CanUnitBean bean = new CanUnitBean();
        bean.setCanId(id);
        bean.setCanTime(Long.parseLong(canTime));
        Map<String, Object> values = new HashMap<>();

        switch (id) {
            case 0x0E:
                values.put("c00E_ido", 1.0F);
                values.put("c00E_keido", 1.0F);
                break;
            case 0x00:
                values.put("c000_ido", 1.0F);
                values.put("c000_keido", 1.0F);
                break;
            case 0x01:
                values.put("c001_dummy15", getRandomValue());
                values.put("c001_dummy16", getRandomValue());
                values.put("c001_dummy17", getRandomValue());
                break;
            case 0x22:
                values.put("c022_dummy15", getRandomValue());
                values.put("c022_dummy16", getRandomValue());
                values.put("c022_dummy17", getRandomValue());
                values.put("c022_dummy18", getRandomValue());
                break;
            case 0x122:
                values.put("c122_dummy15", getRandomValue());
                values.put("c122_dummy16", getRandomValue());
                values.put("c122_dummy17", getRandomValue());
                values.put("c122_dummy18", getRandomValue());
                values.put("c122_dummy19", getRandomValue());
                break;
            case 0x201:
                values.put("c201_dummy15", getRandomValue());
                values.put("c201_dummy16", getRandomValue());
                values.put("c201_dummy17", getRandomValue());
                values.put("c201_dummy18", getRandomValue());
                values.put("c201_dummy19", getRandomValue());
                values.put("c201_dummy20", getRandomValue());
                break;
        }
        bean.setConvertedDataMap(values);
        return bean;
    }

    private static Object getRandomValue() {

        Random random = new Random();

        long a = random.nextInt(10);

        if (a < 3) {
            return null;
        } else {
            return a;
        }
    }

    public static void converedCanUnitBean(CanUnitBean bean, CanUnitBean subcan) {

        short id = bean.getCanId();

        Map<String, Object> values = new HashMap<>();

        switch (id) {
            case 0x0E:
                values.put("c00E_ido", "0x0E");
                values.put("c00E_keido", "0x0E");
                break;
            case 0x00:
                values.put("c000_ido", "0x00");
                values.put("c000_keido", "0x00");
                break;
            case 0x01:
                values.put("c001_dummy15", "0x01");
                values.put("c001_dummy16", "0x01");
                values.put("c001_dummy17", "0x01");
                break;
            case 0x122:
                values.put("c122_dummy15", "0x122");
                values.put("c122_dummy16", "0x122");
                values.put("c122_dummy17", "0x122");
                values.put("c122_dummy18", "0x122");
                values.put("c122_dummy19", "0x122");
                break;
            case 0x201:
                values.put("c201_dummy15", "0x201");
                values.put("c201_dummy16", "0x201");
                values.put("c201_dummy17", "0x201");
                values.put("c201_dummy18", "0x201");
                values.put("c201_dummy19", "0x201");
                values.put("c201_dummy20", "0x201");
                break;
        }

        bean.setConvertedDataMap(values);
    }

    public static void converedCanUnitBeanOf0X22(CanUnitBean bean, CanUnitBean subcan) {

        short id = bean.getCanId();

        Map<String, Object> values = new HashMap<>();

        switch (id) {

            case 0x22:
                values.put("c022_dummy15", subcan.getCanTime());
                values.put("c022_dummy16", subcan.getCanTime());
                values.put("c022_dummy17", subcan.getCanTime());
                values.put("c022_dummy18", subcan.getCanTime());
                break;
        }

        bean.setConvertedDataMap(values);
    }
}
