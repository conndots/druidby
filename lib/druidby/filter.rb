
module Druidby
  class Filter
    def initialize(&block)
      FilterExpression.new(block).to_json
    end
  end

  class FilterDimension
    def initialize(name)
      @dimension = name
    end
  end

  class FilterExpression
    def initialize(&block)
    end

    def to_json()
    end
  end
end
