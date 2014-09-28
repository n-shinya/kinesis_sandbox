require 'twitter'

class TwitterStreamingClient
  def initialize

    @client = Twitter::Streaming::Client.new do |config|
      config.consumer_key = ENV['TWITTER_CONSUMER_KEY']
      config.consumer_secret = ENV['TWITTER_CONSUMER_SECRET']
      config.access_token = ENV['TWITTER_ACCESS_TOKEN']
      config.access_token_secret = ENV['TWITTER_ACCESS_TOKEN_SECRET']
    end
  end

  def request(filtering_words)
    @client.filter(:track => filtering_words, :language => 'ja') do |obj|
      if obj.is_a?(Twitter::Tweet)
        yield obj
      end
    end
  end 
end
