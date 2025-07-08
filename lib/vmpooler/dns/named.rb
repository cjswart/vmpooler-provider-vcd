module Vmpooler
  class PoolManager
    class Dns
      class Named < Vmpooler::PoolManager::DNS::Base
        def initialize(config, logger, metrics, redis_connection_pool, name, options)
          super(config, logger, metrics, redis_connection_pool, name, options)

          task_limit = global_config[:config].nil? || global_config[:config]['task_limit'].nil? ? 10 : global_config[:config]['task_limit'].to_i
          @redis = redis_connection_pool
        end
        def create_or_replace_record(hostname)
          logger.log('s', "[VCD-DNS] #{self.class.name} does not implement create_or_replace_record for #{hostname}")
        end

        def delete_record(hostname)
          logger.log('s', "[VCD-DNS] #{self.class.name} does not implement delete_record for #{hostname}")
        end
      end
    end
  end
end