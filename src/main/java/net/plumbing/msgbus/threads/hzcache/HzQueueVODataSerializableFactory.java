package net.plumbing.msgbus.threads.hzcache;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HzQueueVODataSerializableFactory implements DataSerializableFactory {

    public static final int FACTORY_ID = HzQueueVO.FACTORY_ID; // Используем FACTORY_ID из MessageQueue
    public static final int MESSAGE_QUEUE_ENTRY_CLASS_ID = HzQueueVO.MESSAGE_QUEUE_ENTRY_CLASS_ID;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (typeId) {
            case MESSAGE_QUEUE_ENTRY_CLASS_ID:
                return new HzQueueVO();
            default:
                return null;
        }
    }
}
