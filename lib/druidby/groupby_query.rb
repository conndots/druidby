require 'druidby/query'

module Druidby
  class GroupByQuery < Druidby::Query
    def initialize(client, data_source)
      super(client, data_source)
      @json_data[:queryType] = "groupBy"
      @json_data[:dimensions] = []
    end

    def by(*dimensions)
      @json_data[:dimensions] += dimensions
      self
    end
  end
end
