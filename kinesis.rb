require 'aws-sdk'
require 'parallel'

class Kinesis

  def initialize(stream_name)
    @client = AWS::Kinesis.new(
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    ).client
    @stream_name = stream_name
  end

  def put_record(partition_key, data)
    response = @client.put_record(
      stream_name: @stream_name,
      data: data,
      partition_key: partition_key
    )
    puts "Data: #{data}, Partition Key: #{partition_key}, Shard ID: #{response.shard_id}, Seq Number: #{response.sequence_number}"
  end

  def get_record
    shards = @client.describe_stream(stream_name: @stream_name).stream_description.shards
    shard_ids = shards.map(&:shard_id)
    p shard_ids
    Parallel.each(shard_ids, in_threads: shard_ids.count) do | shard_id |
      shard_iterator_info = @client.get_shard_iterator(
        stream_name: @stream_name,
        shard_id: shard_id,
        shard_iterator_type: 'TRIM_HORIZON'
      )
      shard_iterator = shard_iterator_info.shard_iterator
      loop do
        records_info = @client.get_records(
          shard_iterator: shard_iterator,
          limit: 100
        )
        records_info.records.each do |record|
          puts "Data : #{record.data}, Partition Key : #{record.partition_key}, Shard Id : #{shard_id}, Sequence Number : #{record.sequence_number}"
        end
        shard_iterator = records_info.next_shard_iterator
      end
    end
  end
end
