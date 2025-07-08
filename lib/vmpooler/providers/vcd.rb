# frozen_string_literal: true

require 'bigdecimal'
require 'bigdecimal/util'
require 'rbvmomi'
require 'vmpooler/providers/base'
require_relative '../../vmpooler/cloudapi/cloudapi'

module Vmpooler
  class PoolManager
    class Provider
      class Vcd < Vmpooler::PoolManager::Provider::Base
        # The connection_pool method is normally used only for testing
        attr_reader :connection_pool

        def initialize(config, logger, metrics, redis_connection_pool, name, options)
          super(config, logger, metrics, redis_connection_pool, name, options)

          task_limit = global_config[:config].nil? || global_config[:config]['task_limit'].nil? ? 10 : global_config[:config]['task_limit'].to_i
          # The default connection pool size is:
          # Whatever is biggest from:
          #   - How many pools this provider services
          #   - Maximum number of cloning tasks allowed
          #   - Need at least 2 connections so that a pool can have inventory functions performed while cloning etc.
          default_connpool_size = [provided_pools.count, task_limit, 2].max
          connpool_size = provider_config['connection_pool_size'].nil? ? default_connpool_size : provider_config['connection_pool_size'].to_i
          # The default connection pool timeout should be quite large - 60 seconds
          connpool_timeout = provider_config['connection_pool_timeout'].nil? ? 60 : provider_config['connection_pool_timeout'].to_i
          logger.log('d', "[#{name}] ConnPool - Creating a connection pool of size #{connpool_size} with timeout #{connpool_timeout}")
          @connection_pool = Vmpooler::PoolManager::GenericConnectionPool.new(
            metrics: metrics,
            connpool_type: 'provider_connection_pool',
            connpool_provider: name,
            size: connpool_size,
            timeout: connpool_timeout
          ) do
            logger.log('d', "[#{name}] Connection Pool - Creating a connection object version 1.0.9")
            # Need to wrap the vSphere connection object in another object. The generic connection pooler will preserve
            # the object reference for the connection, which means it cannot "reconnect" by creating an entirely new connection
            # object.  Instead by wrapping it in a Hash, the Hash object reference itself never changes but the content of the
            # Hash can change, and is preserved across invocations.
            new_conn = connect_to_vcd
            { connection: new_conn }
          end
          @provider_hosts = {}
          @provider_hosts_lock = Mutex.new
          @redis = redis_connection_pool
        end

        # name of the provider class
        def name
          'vcd'
        end

        def domain(pool_name)
          dns_plugin_name = pool_config(pool_name)['dns_plugin']
          dns_config(dns_plugin_name)
        end

        def folder_configured?(folder_title, base_folder, configured_folders, allowlist)
          logger.log('d', "Do not use folder_configured any more")
        end

        def destroy_vm_and_log(vm_name, vm_object, pool, data_ttl)
          try = 0 if try.nil?
          max_tries = 3
          @redis.with_metrics do |redis|
            redis.multi do |transaction|
              transaction.srem("vmpooler__completed__#{pool}", vm_name)
              transaction.hdel("vmpooler__active__#{pool}", vm_name)
              transaction.hset("vmpooler__vm__#{vm_name}", 'destroy', Time.now.to_s)

              # Auto-expire metadata key
              transaction.expire("vmpooler__vm__#{vm_name}", (data_ttl * 60 * 60))
            end
          end

          start = Time.now

          if vm_object.is_a? RbVmomi::VIM::Folder
            logger.log('s', "[!] [#{pool}] '#{vm_name}' is a folder, bailing on destroying")
            raise('Expected VM, but received a folder object')
          end
          vm_object.PowerOffVM_Task.wait_for_completion if vm_object.runtime&.powerState && vm_object.runtime.powerState == 'poweredOn'
          vm_object.Destroy_Task.wait_for_completion

          finish = format('%<time>.2f', time: Time.now - start)
          logger.log('s', "[-] [#{pool}] '#{vm_name}' destroyed in #{finish} seconds")
          metrics.timing("destroy.#{pool}", finish)
        rescue RuntimeError
          raise
        rescue StandardError => e
          try += 1
          logger.log('s', "[!] [#{pool}] failed to destroy '#{vm_name}' with an error: #{e}")
          try >= max_tries ? raise : retry
        end

        def destroy_folder_and_children(folder_object)
          logger.log('d', "Do not use destroy_folder_and_children any more")
        end

        def destroy_folder(folder_object)
          logger.log('d', "Do not use destroy_folder any more")
        end

        # Return a list of pool folders
        def pool_folders(provider_name)
          logger.log('d', "Do not use pool_folders any more")
        end

        def get_base_folders(folders)
          logger.log('d', "Do not use get_base_folders any more")
        end

        def purge_unconfigured_resources(allowlist)
          logger.log('d', "Do not use purge_unconfigured_resources any more")
        end

        def get_folder_children(folder_name, connection)
          logger.log('d', "Do not use get_folder_children any more")
        end

        def vms_in_pool(pool_name)
          vms = []
          pool = pool_config(pool_name)
          @connection_pool.with_metrics do |pool_object|
            connection = ensured_vcd_connection(pool_object)
            vms = CloudAPI.get_vms_in_pool(connection, pool)
          end
          vms
        end

        def select_target_hosts(target, cluster, datacenter)
          logger.log('d', "Selecting target hosts for cluster #{cluster} in datacenter #{datacenter}")
        end

        def run_select_hosts(pool_name, target)
          now = Time.now
          max_age = @config[:config]['host_selection_max_age'] || 60
          loop_delay = 5
          datacenter = get_target_datacenter_from_config(pool_name)
          cluster = get_target_cluster_from_config(pool_name)
          raise("cluster for pool #{pool_name} cannot be identified") if cluster.nil?
          raise("datacenter for pool #{pool_name} cannot be identified") if datacenter.nil?

          dc = "#{datacenter}_#{cluster}"
          unless target.key?(dc)
            select_target_hosts(target, cluster, datacenter)
            return
          end
          wait_for_host_selection(dc, target, loop_delay, max_age) if target[dc].key?('checking')
          select_target_hosts(target, cluster, datacenter) if target[dc].key?('check_time_finished') && now - target[dc]['check_time_finished'] > max_age
        end

        def wait_for_host_selection(dc, target, maxloop = 0, loop_delay = 1, max_age = 60)
          loop_count = 1
          until target.key?(dc) && target[dc].key?('check_time_finished')
            sleep(loop_delay)
            unless maxloop == 0
              break if loop_count >= maxloop

              loop_count += 1
            end
          end
          return unless target[dc].key?('check_time_finished')

          loop_count = 1
          while Time.now - target[dc]['check_time_finished'] > max_age
            sleep(loop_delay)
            unless maxloop == 0
              break if loop_count >= maxloop

              loop_count += 1
            end
          end
        end

        def select_next_host(pool_name, target, architecture = nil)
          datacenter = get_target_datacenter_from_config(pool_name)
          cluster = get_target_cluster_from_config(pool_name)
          raise("cluster for pool #{pool_name} cannot be identified") if cluster.nil?
          raise("datacenter for pool #{pool_name} cannot be identified") if datacenter.nil?

          dc = "#{datacenter}_#{cluster}"
          @provider_hosts_lock.synchronize do
            if architecture
              raise("there is no candidate in vcenter that meets all the required conditions, that the cluster has available hosts in a 'green' status, not in maintenance mode and not overloaded CPU and memory") unless target[dc].key?('architectures')

              host = target[dc]['architectures'][architecture].shift
              target[dc]['architectures'][architecture] << host
              if target[dc]['hosts'].include?(host)
                target[dc]['hosts'].delete(host)
                target[dc]['hosts'] << host
              end
            else
              raise("there is no candidate in vcenter that meets all the required conditions, that the cluster has available hosts in a 'green' status, not in maintenance mode and not overloaded CPU and memory") unless target[dc].key?('hosts')

              host = target[dc]['hosts'].shift
              target[dc]['hosts'] << host
              target[dc]['architectures'].each do |arch|
                target[dc]['architectures'][arch] = arch.partition { |v| v != host }.flatten if arch.include?(host)
              end
            end

            return host
          end
        end

        def vm_in_target?(pool_name, parent_host, architecture, target)
          datacenter = get_target_datacenter_from_config(pool_name)
          cluster = get_target_cluster_from_config(pool_name)
          raise("cluster for pool #{pool_name} cannot be identified") if cluster.nil?
          raise("datacenter for pool #{pool_name} cannot be identified") if datacenter.nil?

          dc = "#{datacenter}_#{cluster}"
          raise("there is no candidate in vcenter that meets all the required conditions, that the cluster has available hosts in a 'green' status, not in maintenance mode and not overloaded CPU and memory") unless target[dc].key?('hosts')
          return true if target[dc]['hosts'].include?(parent_host)
          return true if target[dc]['architectures'][architecture].include?(parent_host)

          false
        end
        def check_power_on_and_get_vm(vm_name, pool, connection)
          refreshed_vm_hash = CloudAPI.get_vm(vm_name, connection, pool)
          if refreshed_vm_hash['status'] == 'POWERED_OFF'
            logger.log('d', "VM #{vm_name} is now created but powered_off.")
            logger.log('d', "If specified trying to add security tags to VM #{refreshed_vm_hash['name']}...")
            if pool['security_tags'] && !pool['security_tags'].empty?
              puts "Adding security tags to VM #{refreshed_vm_hash['name']}..."
              security_tags = pool['security_tags']
              puts "Security tags to be added: #{security_tags}"
              security_tags_response = CloudAPI.add_security_tags(refreshed_vm_hash, connection, security_tags)
              if security_tags_response.is_a?(Net::HTTPSuccess)
                puts "Security tags added successfully to VM #{refreshed_vm_hash['name']}."
              else
                puts "\e[31mFailed to add security tags to VM #{refreshed_vm_hash['name']}. Response: #{security_tags_response.body}\e[0m"
              end
            end
            puts "Attempting to power on VM #{vm_name}...#{refreshed_vm_hash['status']}"
            task_href = nil
            task_href = CloudAPI.poweron_vm(refreshed_vm_hash, connection)
            max_wait = 120 # seconds
            waited = 0
            interval = 10
            task_status = CloudAPI.get_task_status(task_href, connection)
            while task_status == 'running' || task_status == 'queued'
              sleep interval
              waited += interval
              task_status = CloudAPI.get_task_status(task_href, connection)
              if waited >= max_wait
                puts "Timeout waiting for VM power on task to finish."
                break
              end
            end
            if task_status == 'success'
              puts "VM #{vm_name} powered on successfully."
              refreshed_vm_hash = CloudAPI.get_vm(vm_name, connection, pool)
            else
              puts "Task Power On VM #{vm_name} failed status after waiting: #{task_status}"
            end
          end
          return refreshed_vm_hash
        end
        def get_vm(pool_name, vm_name)
          vm_hash = nil
          pool = pool_config(pool_name)
          @connection_pool.with_metrics do |pool_object|
            connection = ensured_vcd_connection(pool_object)
            vm_hash = CloudAPI.get_vm(vm_name, connection, pool)
          end
          vm_hash
        end

        def create_vm(pool_name, new_vmname)
          pool = pool_config(pool_name)
          raise("Pool #{pool_name} does not exist for the provider #{name}") if pool.nil?

          vm_hash = nil
          @connection_pool.with_metrics do |pool_object|
            connection = ensured_vcd_connection(pool_object)
            vapp = nil
            vapp = CloudAPI.cloudapi_vapp(pool, connection)
            raise("[VCD provider] Pool #{pool_name} does not exist for the provider #{name}") if vapp.nil?
            # Create a new VM in the vApp
            task_href = nil
            task_href = CloudAPI.cloudapi_create_vm(new_vmname, pool, connection, vapp)
            max_wait = 290 # seconds
            waited = 0
            interval = 10
            task_status = CloudAPI.get_task_status(task_href, connection)
            while task_status == 'running' || task_status == 'queued'
              sleep interval
              waited += interval
              task_status = CloudAPI.get_task_status(task_href, connection)
              if waited >= max_wait
                puts "Timeout waiting for VM creation task to finish."
                break
              end
            end
            if task_status == 'success'
              vm_hash = check_power_on_and_get_vm(new_vmname, pool, connection)
            else
              logger.log('s', "\e[31mTask Create VM #{new_vmname} failed status after waiting: #{task_status}\e[0m")
            end
          end
          vm_hash
        end
        def get_vm_ip_address(vm_name, pool_name)
          vm_hash = nil
          pool = pool_config(pool_name)
          @connection_pool.with_metrics do |pool_object|
            connection = ensured_vcd_connection(pool_object)
            vm_hash = CloudAPI.get_vm(vm_name, connection, pool)
          end
          return vm_hash['ipAddress']
        end

        def create_config_spec(vm_name, template_name, extra_config)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def create_relocate_spec(target_datastore, target_datacenter_name, pool_name, connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def create_clone_spec(relocate_spec, config_spec)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def set_network_device(datacenter_name, template_vm_network_device, network_name, connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def create_disk(pool_name, vm_name, disk_size)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def create_snapshot(pool_name, vm_name, new_snapshot_name)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def revert_snapshot(pool_name, vm_name, snapshot_name)
          raise("#{self.class.name} not implemented in vcd provider")
        end
        def destroy_vm_and_log(vm_name, vm_object, pool, data_ttl)
          try = 0 if try.nil?
          max_tries = 3
          @redis.with_metrics do |redis|
            redis.multi do |transaction|
              transaction.srem("vmpooler__completed__#{pool}", vm_name)
              transaction.hdel("vmpooler__active__#{pool}", vm_name)
              transaction.hset("vmpooler__vm__#{vm_name}", 'destroy', Time.now.to_s)

              # Auto-expire metadata key
              transaction.expire("vmpooler__vm__#{vm_name}", (data_ttl * 60 * 60))
            end
          end

          start = Time.now
          destroy_vm(pool['name'], vm_name)
          finish = format('%<time>.2f', time: Time.now - start)
          logger.log('s', "[-] [#{pool}] '#{vm_name}' destroyed in #{finish} seconds")
          metrics.timing("destroy.#{pool}", finish)
        rescue RuntimeError
          raise
        rescue StandardError => e
          try += 1
          logger.log('s', "[!] [#{pool}] failed to destroy '#{vm_name}' with an error: #{e}")
          try >= max_tries ? raise : retry
        end
        def destroy_vm(pool_name, vm_name)
          pool = pool_config(pool_name)
          raise("Pool #{pool_name} does not exist for the provider #{name}") if pool.nil?
          result = false

          @connection_pool.with_metrics do |pool_object|
            connection = ensured_vcd_connection(pool_object)

            vm_hash = CloudAPI.get_vm(vm_name, connection, pool)
            raise("VM #{vm_name} in pool #{pool_name} does not exist for the provider #{name}") if vm_hash.nil?
            result = CloudAPI.destroy_vm(vm_hash, connection)
          end
          return result
        end

        # mock vm_ready till we are on the rigth vmpooler server with working dns
        def vm_ready?(pool_name, vm_name, redis)
          begin
            domain = domain(pool_name)
            open_socket(vm_name, domain)
          rescue StandardError => e
            redis.hset("vmpooler__vm__#{vm_name}", 'open_socket_error', e.to_s)
            puts "\e[31mVM #{vm_name} is not ready: #{e}\e[0m"
            return true
          end
          redis.hdel("vmpooler__vm__#{vm_name}", 'open_socket_error')
          true
        end

        # Return a hash of VM data
        # Provides vmname, hostname, template, poolname, boottime and powerstate information
        def generate_vm_hash(vm_object, pool_name)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def ensured_vcd_connection(connection_pool_object)
          connection_pool_object[:connection] = connect_to_vcd unless CloudAPI.cloudapi_check_session(connection_pool_object[:connection])
          connection_pool_object[:connection]
        end

        def connect_to_vcd
          max_tries = global_config[:config]['max_tries'] || 3
          retry_factor = global_config[:config]['retry_factor'] || 10
          try = 1
          begin
            connection = CloudAPI.cloudapi_login(provider_config['vcloud_url'],provider_config['auth_encoded'], provider_config['api_version'])
            connection[:vdc_href] = "#{provider_config['vcloud_url']}/api/vdc/#{provider_config['vdc_id']}"
            connection[:vdc_id] = provider_config['vdc_id']

            metrics.increment('connect.open')
            connection
          rescue StandardError => e
            metrics.increment('connect.fail')
            raise e if try >= max_tries

            sleep(try * retry_factor)
            try += 1
            retry
          end
        end

        # This should supercede the open_socket method in the Pool Manager
        def open_socket(host, domain = nil, timeout = 5, port = 22, &_block)
          target_host = host
          target_host = "#{host}.#{domain}" if domain
          sock = TCPSocket.new(target_host, port, connect_timeout: timeout)
          begin
            puts "\e[33m#{sock.inspect}\e[0m"
            yield sock if block_given?
          ensure
            sock.close
          end
        end

        # Returns an array containing cumulative CPU and memory utilization of a host, and its object reference
        # Params:
        # +model+:: CPU arch version to match on
        # +limit+:: Hard limit for CPU or memory utilization beyond which a host is excluded for deployments
        # returns nil if one on these conditions is true:
        #    the model param is defined and cannot be found
        #    the host is in maintenance mode
        #    the host status is not 'green'
        #    the cpu or memory utilization is bigger than the limit param
        def get_host_utilization(host, model = nil, limit = 90)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def host_has_cpu_model?(host, model)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def get_host_cpu_arch_version(host)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def cpu_utilization_for(host)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def memory_utilization_for(host)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def get_average_cluster_utilization(hosts)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def build_compatible_hosts_lists(hosts, percentage)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def select_least_used_hosts(hosts, percentage)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def find_least_used_hosts(cluster, datacentername, percentage)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def find_host_by_dnsname(connection, dnsname)
          host_object = connection.searchIndex.FindByDnsName(dnsName: dnsname, vmSearch: false)
          return nil if host_object.nil?

          host_object
        end

        def find_least_used_host(cluster, connection, datacentername)
          logger.log('d', "Initialize find_least_used_host for cluster #{cluster} in datacenter #{datacentername}")
          target_hosts.min[1]
        end

        def find_least_used_vpshere_compatible_host(vm)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def find_snapshot(vm, snapshotname)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def build_propSpecs(datacenter, folder, vmname) # rubocop:disable Naming/MethodName
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def find_vm(pool_name, vmname, connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def get_base_vm_container_from(connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def get_snapshot_list(tree, snapshotname)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def get_vm_details(pool_name, vm_name, connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def migration_enabled?(config)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def migrate_vm(pool_name, vm_name)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def migrate_vm_to_new_host(pool_name, vm_name, vm_hash, connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def migrate_vm_and_record_timing(pool_name, vm_name, vm_hash, target_host_object, dest_host_name)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def migrate_vm_host(vm_object, host)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def create_folder(connection, new_folder, datacenter)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def find_template_vm(pool, connection)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def create_template_delta_disks(pool)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def valid_template_path?(template)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def get_disk_backing(pool)
          raise("#{self.class.name} not implemented in vcd provider")
        end

        def linked_clone?(pool)
          raise("#{self.class.name} not implemented in vcd provider")
        end
      end
    end
  end
end
