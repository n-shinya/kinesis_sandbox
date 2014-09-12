require 'json'
require './twitter'
require './kinesis'

stream_name=ARGV[0]
filtering_words=ARGV[1]
abort if stream_name.nil? || filtering_words.nil?

kinesis_client = Kinesis.new(stream_name)

TwitterStreamingClient.new.request(filtering_words) do |res|
  data = {
    :id => res.id,
    :text => res.text,
    :friends_count => res.user.friends_count,
    :followers_count => res.user.followers_count,
    :created_at => res.created_at
  }.to_json
  kinesis_client.put_record(res.id.to_s, data)
end
