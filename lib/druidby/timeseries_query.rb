require 'druid/query'

module Druidby
  class TimeseriesQuery < Druidby::Query
    def initialize(client, data_source, descending = false)
      super(client, data_source)
      @json_data[:queryType] = "timeseries"
      @json_data[:descending] = descending
    end
  end
end
