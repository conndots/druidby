require 'druidby'

module Druidby
  class Client
    attr_accessor :query_url, :timeout
    def initialize(query_url, timeout=60)
      @query_url = query_url
      @timeout = timeout
    end

    def timeseries(data_source, descending=false)
      TimeseriesQuery.new(self, data_source, descending)
    end

    def groupby(data_source)
      GroupByQuery.new(self, data_source)
    end
  end
end
