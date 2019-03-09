# frozen_string_literal: true

module ActiveRecord
  class InsertAll
    attr_reader :model, :connection, :inserts, :on_duplicate, :returning, :unique_by_index

    def initialize(model, inserts, on_duplicate:, returning: nil, unique_by_index: nil)
      raise ArgumentError, "Empty list of attributes passed" if inserts.blank?

      @model, @connection, @inserts, @on_duplicate, @returning = model, model.connection, inserts, on_duplicate, returning

      @returning = (connection.supports_insert_returning? ? primary_keys : false) if @returning.nil?
      @returning = false if @returning == []

      @on_duplicate = :skip if @on_duplicate == :update && updatable_columns.empty?

      @unique_by_index = find_index_for(unique_by_index) if unique_by_index

      ensure_valid_options_for_connection!
    end

    def execute
      connection.exec_query to_sql, "Bulk Insert"
    end

    def keys
      inserts.first.keys.map(&:to_s)
    end

    def updatable_columns
      keys - readonly_columns - unique_by_columns
    end

    def primary_keys
      Array(model.primary_key)
    end


    def skip_duplicates?
      on_duplicate == :skip
    end

    def update_duplicates?
      on_duplicate == :update
    end

    private
      def ensure_valid_options_for_connection!
        if returning && !connection.supports_insert_returning?
          raise ArgumentError, "#{connection.class} does not support :returning"
        end

        unless %i{ raise skip update }.member?(on_duplicate)
          raise NotImplementedError, "#{on_duplicate.inspect} is an unknown value for :on_duplicate. Valid values are :raise, :skip, and :update"
        end

        if on_duplicate == :skip && !connection.supports_insert_on_duplicate_skip?
          raise ArgumentError, "#{connection.class} does not support skipping duplicates"
        end

        if on_duplicate == :update && !connection.supports_insert_on_duplicate_update?
          raise ArgumentError, "#{connection.class} does not support upsert"
        end

        if unique_by_index && !connection.supports_insert_conflict_target?
          raise ArgumentError, "#{connection.class} does not support :unique_by"
        end
      end

      def to_sql
        connection.build_insert_sql(ActiveRecord::InsertAll::Builder.new(self))
      end

      def readonly_columns
        primary_keys + model.readonly_attributes.to_a
      end

      def unique_by_columns
        Array(unique_by_index&.columns)
      end

      def find_index_for(unique_by_index)
        if index = indexes_by_name[unique_by_index.to_s]
          index
        else
          raise ArgumentError, "No suitable index found for #{unique_by_index}"
        end
      end

      def indexes_by_name
        # TODO: use connection.schema_cache.indexes instead.
        connection.indexes(model.table_name).index_by(&:name)
      end

      class Builder
        attr_reader :model

        delegate :skip_duplicates?, :update_duplicates?, to: :insert_all

        def initialize(insert_all)
          @insert_all, @model, @connection = insert_all, insert_all.model, insert_all.connection
        end

        def into
          "INTO #{model.quoted_table_name}(#{columns_list})"
        end

        def values_list
          columns = connection.schema_cache.columns_hash(model.table_name)
          keys = insert_all.keys.to_set
          types = keys.map { |key| [ key, connection.lookup_cast_type_from_column(columns[key]) ] }.to_h

          values_list = insert_all.inserts.map do |attributes|
            attributes = attributes.stringify_keys

            unless attributes.keys.to_set == keys
              raise ArgumentError, "All objects being inserted must have the same keys"
            end

            keys.map do |key|
              bind = Relation::QueryAttribute.new(key, attributes[key], types[key])
              connection.with_yaml_fallback(bind.value_for_database)
            end
          end

          Arel::InsertManager.new.create_values_list(values_list).to_sql
        end

        def returning
          quote_columns(insert_all.returning).join(",") if insert_all.returning
        end

        def conflict_target
          if index = insert_all.unique_by_index
            sql = +"(#{quote_columns(index.columns).join(',')})"
            sql << " WHERE #{index.where}" if index.where
            sql
          elsif update_duplicates?
            "(#{quote_columns(insert_all.primary_keys).join(',')})"
          end
        end

        def updatable_columns
          quote_columns(insert_all.updatable_columns)
        end

        private
          attr_reader :connection, :insert_all

          def columns_list
            quote_columns(insert_all.keys).join(",")
          end

          def quote_columns(columns)
            columns.map(&connection.method(:quote_column_name))
          end
      end
  end
end
