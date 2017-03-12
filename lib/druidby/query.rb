require 'typhoeus'
require 'json'
require 'druidby/filter'

module Druidby
  class query
    def initialize(client, data_source)
      @client = client
      @agg_names = []
      @json_data = {
        :dataSource => data_source
      }
    end

    def between(start_time, end_time=Time.now)
      @json_data[:intervals] = [] if !@json_data.include? :intervals
      if start_time.instance_of?(Time) && end_time.instance_of?(Time)
        @json_data[:intervals] << "#{start_time.iso8601}/#{end_time.iso8601}"
      end
      self
    end

    def filter(filter_expression)
      return self if filter_expression.nil? || filter_expression.size < 2
      @json_data[:filter] = expression_to_json(filter_expression)
      self
    end

    def granularity(granularity)
      if granularity.instance_of? Hash
        @json_data[:granularity] = granularity
      elsif granularity.instance_of? Fixnum
        @json_data[:granularity] = {
          :type => "duration",
          :duration => granularity.to_i * 1000
        }
      elsif granularity.instance_of?(String) || granularity.instance_of?(Symbol)
        @json_data[:granularity] = granularity
      end
      self
    end

    def append_aggregation(type, name, field_name = nil)
      if type.nil? || name.nil?
        return self
      end

      @agg_names << name
      @json_data[:aggregations] = [] if !@json_data.include? :aggregations
      if field_name.nil?
        agg_data = {
          :type => type,
          :name => name
        }
      else
        agg_data = {
          :type => type,
          :name => name,
          :fieldName => field_name
        }
      end
      @json_data[:aggregations] << agg_data
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
      result = !response.timed_out? && response.code == 200 ? JSON.load(response.body) : []

      result.map! do |elem|

        case @json_data[:queryType]
        when "timeseries"
          data_key = "result"
          new_elem = { }
        when "groupBy"
          data_key = "event"
          new_elem = { :dimensions => { } }
          @json_data[:dimensions].each do |d|
            if elem[data_key].include? d.to_s
              new_elem[:dimensions][d.to_s] = elem[data_key][d.to_s]
            end
          end
        else
          data_key = ""
        end

        if elem.include? "timestamp"
          new_elem[:timestamp] = DateTime.parse(elem["timestamp"]).to_time.localtime
        end
        @agg_names.each do |n|
          if elem[data_key].include?(n.to_s)
            new_elem[n.to_sym] = elem[data_key][n.to_s]
          end
        end

        new_elem
      end

      result
    end

    private
    def expression_to_json(expression)
      opr = expression[0]

      case opr
      when :&, :|
        other_exps = expression[1..-1]
        fjson = {
          :type => opr == :& ? "and" : "or",
          :fields => []
        }
        other_exps.each { |exp| fjson[:fields] << expression_to_json(exp) }
        fjson
      when :!
        {
          :type => "not",
          :fields => [expression_to_json(expression[1])]
        }
      when :in
        {
          :type => "in",
          :dimension => expression[1],
          :values => expression[2]
        }
      when :eq
        {
          :type => "selector",
          :dimension => expression[1],
          :value => expression[2]
        }
      else
        nil
      end
    end
  end
end
