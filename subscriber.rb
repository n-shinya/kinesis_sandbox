require './kinesis'

stream_name = ARGV[0]
abort if stream_name.nil?

Kinesis.new(stream_name).get_record
