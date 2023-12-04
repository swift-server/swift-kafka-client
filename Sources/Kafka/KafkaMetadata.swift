import Crdkafka

public final class KafkaMetadata {
    private let metadata: UnsafePointer<rd_kafka_metadata>
    
    init(metadata: UnsafePointer<rd_kafka_metadata>) {
        self.metadata = metadata
    }
    
    deinit {
        rd_kafka_metadata_destroy(metadata)
    }
    
    public private(set) lazy var topics = {
        (0..<Int(self.metadata.pointee.topic_cnt)).map { KafkaTopicMetadata(metadata: self, topic: self.metadata.pointee.topics[$0]) }
    }()
}

// must be a class to allow mutating lazy vars, otherwise require struct copies
public final class KafkaTopicMetadata {
    private let metadata: KafkaMetadata // retain metadata
    private let topic: rd_kafka_metadata_topic
    
    init(metadata: KafkaMetadata, topic: rd_kafka_metadata_topic) {
        self.metadata = metadata
        self.topic = topic
    }

    public private(set) lazy var name = {
        String(cString: self.topic.topic)
    }()
    
    public private(set) lazy var partitions = {
        (0..<Int(self.topic.partition_cnt)).map { KafkaPartitionMetadata(metadata: self.metadata, partition: topic.partitions[$0]) }
    }()
}

public struct KafkaPartitionMetadata {
    private let metadata: KafkaMetadata // retain metadata
    private let partition: rd_kafka_metadata_partition
    
    init(metadata: KafkaMetadata, partition: rd_kafka_metadata_partition) {
        self.metadata = metadata
        self.partition = partition
    }
    
    var id: Int {
        Int(partition.id)
    }

    var replicasCount: Int {
        Int(partition.replica_cnt)
    }
}
