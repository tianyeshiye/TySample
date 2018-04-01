package avro;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.avro.generic.GenericData;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

public class MyKryoRegistrator implements KryoRegistrator {

	  public static class SpecificInstanceCollectionSerializer<T extends Collection> extends CollectionSerializer {
	    Class<T> type;
	    public SpecificInstanceCollectionSerializer(Class<T> type) {
	      this.type = type;
	    }

	    @Override
	    protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
	      return kryo.newInstance(this.type);
	    }

	    @Override
	    protected Collection createCopy(Kryo kryo, Collection original) {
	      return kryo.newInstance(this.type);
	    }
	  }


	  @Override
	  public void registerClasses(Kryo kryo) {
	    // Avro POJOs contain java.util.List which have GenericData.Array as their runtime type
	    // because Kryo is not able to serialize them properly, we use this serializer for them
	    kryo.register(GenericData.Array.class, new SpecificInstanceCollectionSerializer<>(ArrayList.class));
	    kryo.register(canInfo.class);
	  }
	}