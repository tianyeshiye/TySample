package sample.spark.streaming.spark.avro;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import sample.spark.streaming.CanUnitBean;

public class AvroSchemaMapper implements Serializable{

	public Schema getSchema() {
		Schema schema;
		try {
			schema = new Schema.Parser().parse(new File("D:/workspace_spark/TySample/src/main/java/sample/spark/streaming/spark/avro/canInfo.avsc"));
		} catch (IOException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return schema;
	}

	public GenericRecord map(List<CanUnitBean> canUnitBeanList) {
		GenericRecord record = new GenericData.Record(getSchema());
		record.put("canId", String.valueOf(canUnitBeanList.get(0).getCanId()));
		record.put("canTime", canUnitBeanList.get(0).getCanTime());
        return record;
	}
}
