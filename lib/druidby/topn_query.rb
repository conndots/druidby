require 'druidby/query'

module Druidby
  class GroupByQuery < Query
    def initialize(client, data_source)
      super(client, data_source)
      @json_data[:queryType] = "groupBy"
    end

    def by(*dimensions)
      @json_data[:dimensions] = dimensions
      self
    end
  end
end
