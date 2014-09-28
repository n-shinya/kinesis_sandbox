require 'json'
require './twitter'
require './kinesis'

stream_name=ARGV[0]
filtering_words=ARGV[1]
abort if stream_name.nil? || filtering_words.nil?

kinesis_client = Kinesis.new(stream_name)

TwitterStreamingClient.new.request(filtering_words) do |res|
  id = res.id.to_s
  res_json = res.to_h.to_json
  kinesis_client.put_record(id, res_json)
end
