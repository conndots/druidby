require 'typhoeus'
require 'json'

module Druidby
  class query
    def initialize(client, data_source)
      @client = client
      @json_data = {
        :dataSource => data_source
      }
    end

    def filter(&block)
      #TODO
      self
    end

    def aggregations(&block)
      #TODO
      self
    end

    def post_aggregations(&block)
      #TODO
      self
    end

    def having(&block)
      #TODO
      self
    end

    def granularity(gradularity=nil, &block)
      #TODO
      self
    end

    def between(start_time, end_time=Time.now)
      @json_data[:intervals] = [] if !@json_data.include? :intervals
      @json_data[:intervals] << "#{start_time.iso8601}/#{end_time.iso8601}"
      self
    end

    def get_response()
      response = Typhoeus::Request.post(
        @client.query_url,
        headers: {
          "Content-Type" => "application/json"
        },
        body: JSON.dump(@json_data),
        timeout: @client.timeout
      )
      !response.timed_out? || response.code == 200 ? JSON.load(response.body) : {}
    end
  end
end
