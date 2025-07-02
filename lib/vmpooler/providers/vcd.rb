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
        def wait_for_vm_creation(vm_name, pool, connection)
          max_wait = 120 # seconds
          waited = 0
          interval = 10
          loop do
            sleep interval
            waited += interval
            puts "\e[31mWaiting for VM #{vm_name} to be created... (#{waited}/#{max_wait} seconds)\e[0m"
            refreshed_vm_hash = CloudAPI.get_vm(vm_name, connection, pool)
            puts "Current status of VM #{vm_name}: #{refreshed_vm_hash['status']}"
            if refreshed_vm_hash['status'] == 'POWERED_OFF'
              puts "VM #{vm_name} is now created but powered_off."
                puts "Attempting to power on VM #{vm_name}..."
              sleep 20 # Give it a moment to settle
              power_on_response = CloudAPI.poweron_vm(refreshed_vm_hash, connection)
              if power_on_response.is_a?(Net::HTTPSuccess)
                puts "VM #{refreshed_vm_hash['name']} powered on successfully."
                puts "Waiting 30 seconds for VM #{refreshed_vm_hash['name']} to be fully operational..."
                15.times do
                  sleep 2
                  print '.'
                end
              else
                puts "\e[31mFailed to power on VM #{refreshed_vm_hash['name']}. Response: #{power_on_response.body}\e[0m"
              end
              return refreshed_vm_hash
            end
            if waited >= max_wait
                puts "Timeout waiting for VM #{refreshed_vm_hash['name']} to be created."
                return refreshed_vm_hash
            end
            if refreshed_vm_hash['status'] == 'POWERED_ON'
              puts "VM #{refreshed_vm_hash['name']} is powered on."
              return refreshed_vm_hash
            end
          end
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
            CloudAPI.cloudapi_create_vm(new_vmname, pool, connection, vapp)
            # Wait for the VM to be created
            vm_hash = wait_for_vm_creation(new_vmname, pool, connection)
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

        # def create_config_spec(vm_name, template_name, extra_config)
          # RbVmomi::VIM.VirtualMachineConfigSpec(
            # annotation: JSON.pretty_generate(
              # name: vm_name,
              # created_by: provider_config['username'],
              # base_template: template_name,
              # creation_timestamp: Time.now.utc
            # ),
            # extraConfig: extra_config
          # )
        # end

        # def create_relocate_spec(target_datastore, target_datacenter_name, pool_name, connection)
          # pool = pool_config(pool_name)
          # target_cluster_name = get_target_cluster_from_config(pool_name)
#
          # relocate_spec = RbVmomi::VIM.VirtualMachineRelocateSpec(
            # datastore: find_datastore(target_datastore, connection, target_datacenter_name),
            # diskMoveType: get_disk_backing(pool)
          # )
          # manage_host_selection = @config[:config]['manage_host_selection'] if @config[:config].key?('manage_host_selection')
          # if manage_host_selection
            # run_select_hosts(pool_name, @provider_hosts)
            # target_host = select_next_host(pool_name, @provider_hosts)
            # host_object = find_host_by_dnsname(connection, target_host)
            # relocate_spec.host = host_object
          # else
          # # Choose a cluster/host to place the new VM on
            # target_cluster_object = find_cluster(target_cluster_name, connection, target_datacenter_name)
            # relocate_spec.pool = target_cluster_object.resourcePool
          # end
          # relocate_spec
        # end

        # def create_clone_spec(relocate_spec, config_spec)
          # RbVmomi::VIM.VirtualMachineCloneSpec(
            # location: relocate_spec,
            # config: config_spec,
            # powerOn: true,
            # template: false
          # )
        # end

        # def set_network_device(datacenter_name, template_vm_network_device, network_name, connection)
          # # Retrieve network object
          # datacenter = connection.serviceInstance.find_datacenter(datacenter_name)
          # new_network = datacenter.network.find { |n| n.name == network_name }
#
          # raise("Cannot find network #{network_name} in datacenter #{datacenter_name}") unless new_network
#
          # # Determine network device type
          # # All possible device type options here: https://vdc-download.vmware.com/vmwb-repository/dcr-public/98d63b35-d822-47fe-a87a-ddefd469df06/2e3c7b58-f2bd-486e-8bb1-a75eb0640bee/doc/vim.vm.device.VirtualEthernetCard.html
          # network_device =
            # if template_vm_network_device.instance_of? RbVmomi::VIM::VirtualVmxnet2
              # RbVmomi::VIM.VirtualVmxnet2
            # elsif template_vm_network_device.instance_of? RbVmomi::VIM::VirtualVmxnet3
              # RbVmomi::VIM.VirtualVmxnet3
            # elsif template_vm_network_device.instance_of? RbVmomi::VIM::VirtualE1000
              # RbVmomi::VIM.VirtualE1000
            # elsif template_vm_network_device.instance_of? RbVmomi::VIM::VirtualE1000e
              # RbVmomi::VIM.VirtualE1000e
            # elsif template_vm_network_device.instance_of? RbVmomi::VIM::VirtualSriovEthernetCard
              # RbVmomi::VIM.VirtualSriovEthernetCard
            # else
              # RbVmomi::VIM.VirtualPCNet32
            # end

          # Set up new network device attributes
          # network_device.key = template_vm_network_device.key
          # network_device.deviceInfo = RbVmomi::VIM.Description(
            # label: template_vm_network_device.deviceInfo.label,
            # summary: network_name
          # )
          # network_device.backing = RbVmomi::VIM.VirtualEthernetCardNetworkBackingInfo(
            # deviceName: network_name,
            # network: new_network,
            # useAutoDetect: false
          # )
          # network_device.addressType = 'assigned'
          # network_device.connectable = RbVmomi::VIM.VirtualDeviceConnectInfo(
            # allowGuestControl: true,
            # startConnected: true,
            # connected: true
          # )
          # network_device
        # end

        # def create_disk(pool_name, vm_name, disk_size)
          # pool = pool_config(pool_name)
          # raise("CJS -create-disk- Pool #{pool_name} does not exist for the provider #{name}") if pool.nil?
#
          # datastore_name = pool['datastore']
          # raise("Pool #{pool_name} does not have a datastore defined for the provider #{name}") if datastore_name.nil?
#
          # @connection_pool.with_metrics do |pool_object|
            # connection = ensured_vcd_connection(pool_object)
            # vm_object = find_vm(pool_name, vm_name, connection)
            # raise("VM #{vm_name} in pool #{pool_name} does not exist for the provider #{name}") if vm_object.nil?
#
            # add_disk(vm_object, disk_size, datastore_name, connection, get_target_datacenter_from_config(pool_name))
          # end
          # true
        # end

        # def create_snapshot(pool_name, vm_name, new_snapshot_name)
          # @connection_pool.with_metrics do |pool_object|
            # connection = ensured_vcd_connection(pool_object)
            # vm_object = find_vm(pool_name, vm_name, connection)
            # raise("VM #{vm_name} in pool #{pool_name} does not exist for the provider #{name}") if vm_object.nil?
#
            # old_snap = find_snapshot(vm_object, new_snapshot_name)
            # raise("Snapshot #{new_snapshot_name} for VM #{vm_name} in pool #{pool_name} already exists for the provider #{name}") unless old_snap.nil?
#
            # vm_object.CreateSnapshot_Task(
              # name: new_snapshot_name,
              # description: 'vmpooler',
              # memory: true,
              # quiesce: true
            # ).wait_for_completion
          # end
          # true
        # end

        # def revert_snapshot(pool_name, vm_name, snapshot_name)
          # @connection_pool.with_metrics do |pool_object|
            # connection = ensured_vcd_connection(pool_object)
            # vm_object = find_vm(pool_name, vm_name, connection)
            # raise("VM #{vm_name} in pool #{pool_name} does not exist for the provider #{name}") if vm_object.nil?
#
            # snapshot_object = find_snapshot(vm_object, snapshot_name)
            # raise("Snapshot #{snapshot_name} for VM #{vm_name} in pool #{pool_name} does not exist for the provider #{name}") if snapshot_object.nil?
#
            # snapshot_object.RevertToSnapshot_Task.wait_for_completion
          # end
          # true
        # end
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

        # def vm_ready?(pool_name, vm_name, redis)
        #   begin
        #     domain = domain(pool_name)
        #     open_socket(vm_name, domain)
        #   rescue StandardError => e
        #     redis.hset("vmpooler__vm__#{vm_name}", 'open_socket_error', e.to_s)
        #     return false
        #   end
        #   redis.hdel("vmpooler__vm__#{vm_name}", 'open_socket_error')
        #   true
        # end

        # mock vm_ready till we are on the rigth vmpooler server
        def vm_ready?(pool_name, vm_name, redis)
          domain = domain(pool_name)
          redis.hdel("vmpooler__vm__#{vm_name}", 'open_socket_error')
          true
        end

        # VSphere Helper methods

        # def get_target_cluster_from_config(pool_name)
          # pool = pool_config(pool_name)
          # return nil if pool.nil?
#
          # return pool['clone_target'] unless pool['clone_target'].nil?
          # return global_config[:config]['clone_target'] unless global_config[:config]['clone_target'].nil?
#
          # nil
        # end

        # def get_target_datacenter_from_config(pool_name)
          # pool = pool_config(pool_name)
          # return nil if pool.nil?
#
          # return pool['datacenter']            unless pool['datacenter'].nil?
          # return provider_config['datacenter'] unless provider_config['datacenter'].nil?
#
          # nil
        # end

        # Return a hash of VM data
        # Provides vmname, hostname, template, poolname, boottime and powerstate information
        # def generate_vm_hash(vm_object, pool_name)
          # pool_configuration = pool_config(pool_name)
          # return nil if pool_configuration.nil?
#
          # hostname = vm_object.summary.guest.hostName if vm_object.summary&.guest && vm_object.summary.guest.hostName
          # boottime = vm_object.runtime.bootTime if vm_object.runtime&.bootTime
          # powerstate = vm_object.runtime.powerState if vm_object.runtime&.powerState
#
          # ip_maxloop = 240
          # ip_loop_delay = 1
          # ip_loop_count = 1
          # ip = nil
          # invalid_addresses = /(0|169)\.(0|254)\.\d+\.\d+/
          # while ip.nil?
            # sleep(ip_loop_delay)
            # ip = vm_object.guest_ip
            # ip = nil if !ip.nil? && ip.match?(invalid_addresses)
            # unless ip_maxloop == 0
              # break if ip_loop_count >= ip_maxloop
#
              # ip_loop_count += 1
            # end
          # end
#
          # {
            # 'name' => vm_object.name,
            # 'hostname' => hostname,
            # 'template' => pool_configuration['template'],
            # 'poolname' => pool_name,
            # 'boottime' => boottime,
            # 'powerstate' => powerstate,
            # 'ip' => ip
          # }
        # end

        # vSphere helper methods
        ADAPTER_TYPE = 'lsiLogic'
        DISK_TYPE = 'thin'
        DISK_MODE = 'persistent'

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
            yield sock if block_given?
          ensure
            sock.close
          end
        end

#        def get_vm_folder_path(vm_object)
#          # This gives an array starting from the root Datacenters folder all the way to the VM
#          # [ [Object, String], [Object, String ] ... ]
#          # It's then reversed so that it now goes from the VM to the Datacenter
#          full_path = vm_object.path.reverse
#
#          # Find the Datacenter object
#          dc_index = full_path.index { |p| p[0].is_a?(RbVmomi::VIM::Datacenter) }
#          return nil if dc_index.nil?
#          # The Datacenter should be at least 2 otherwise there's something
#          # wrong with the array passed in
#          # This is the minimum:
#          # [ VM (0), VM ROOT FOLDER (1), DC (2)]
#          return nil if dc_index <= 1
#
#          # Remove the VM name (Starting position of 1 in the slice)
#          # Up until the Root VM Folder of DataCenter Node (dc_index - 2)
#          full_path = full_path.slice(1..dc_index - 2)
#
#          # Reverse the array back to normal and
#          # then convert the array of paths into a '/' seperated string
#          (full_path.reverse.map { |p| p[1] }).join('/')
#        end

        # def add_disk(vm, size, datastore, connection, datacentername)
          # return false unless size.to_i > 0
#
          # vmdk_datastore = find_datastore(datastore, connection, datacentername)
          # raise("Datastore '#{datastore}' does not exist in datacenter '#{datacentername}'") if vmdk_datastore.nil?
#
          # datacenter = connection.serviceInstance.find_datacenter(datacentername)
          # controller = find_disk_controller(vm)
          # disk_unit_number = find_disk_unit_number(vm, controller)
          # disk_count = vm.config.hardware.device.grep(RbVmomi::VIM::VirtualDisk).count
          # vmdk_file_name = "#{vm['name']}/#{vm['name']}_#{disk_count}.vmdk"
#
          # vmdk_spec = RbVmomi::VIM::FileBackedVirtualDiskSpec(
            # capacityKb: size.to_i * 1024 * 1024,
            # adapterType: ADAPTER_TYPE,
            # diskType: DISK_TYPE
          # )
#
          # vmdk_backing = RbVmomi::VIM::VirtualDiskFlatVer2BackingInfo(
            # datastore: vmdk_datastore,
            # diskMode: DISK_MODE,
            # fileName: "[#{datastore}] #{vmdk_file_name}"
          # )
#
          # device = RbVmomi::VIM::VirtualDisk(
            # backing: vmdk_backing,
            # capacityInKB: size.to_i * 1024 * 1024,
            # controllerKey: controller.key,
            # key: -1,
            # unitNumber: disk_unit_number
          # )
#
          # device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
            # device: device,
            # operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
          # )
#
          # vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
            # deviceChange: [device_config_spec]
          # )
#
          # connection.serviceContent.virtualDiskManager.CreateVirtualDisk_Task(
            # datacenter: datacenter,
            # name: "[#{datastore}] #{vmdk_file_name}",
            # spec: vmdk_spec
          # ).wait_for_completion
#
          # vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
#
          # true
        # end

        # def find_datastore(datastorename, connection, datacentername)
          # datacenter = connection.serviceInstance.find_datacenter(datacentername)
          # raise("Datacenter #{datacentername} does not exist") if datacenter.nil?
#
          # datacenter.find_datastore(datastorename)
        # end

        # def find_device(vm, device_name)
          # vm.config.hardware.device.each do |device|
            # return device if device.deviceInfo.label == device_name
          # end
#
          # nil
        # end

        # def find_disk_controller(vm)
          # devices = find_disk_devices(vm)
#
          # devices.keys.sort.each do |device|
            # return find_device(vm, devices[device]['device'].deviceInfo.label) if devices[device]['children'].length < 15
          # end
#
          # nil
        # end

        # def find_disk_devices(vm)
          # devices = {}
#
          # vm.config.hardware.device.each do |device|
            # if device.is_a? RbVmomi::VIM::VirtualSCSIController
              # if devices[device.controllerKey].nil?
                # devices[device.key] = {}
                # devices[device.key]['children'] = []
              # end
#
              # devices[device.key]['device'] = device
            # end
#
            # if device.is_a? RbVmomi::VIM::VirtualDisk
              # if devices[device.controllerKey].nil?
                # devices[device.controllerKey] = {}
                # devices[device.controllerKey]['children'] = []
              # end
#
              # devices[device.controllerKey]['children'].push(device)
            # end
          # end
#
          # devices
        # end

        # def find_disk_unit_number(vm, controller)
          # used_unit_numbers = []
          # available_unit_numbers = []
#
          # devices = find_disk_devices(vm)
#
          # devices.keys.sort.each do |c|
            # next unless controller.key == devices[c]['device'].key
#
            # used_unit_numbers.push(devices[c]['device'].scsiCtlrUnitNumber)
            # devices[c]['children'].each do |disk|
              # used_unit_numbers.push(disk.unitNumber)
            # end
          # end
#
          # (0..15).each do |scsi_id|
            # available_unit_numbers.push(scsi_id) if used_unit_numbers.grep(scsi_id).length <= 0
          # end
#
          # available_unit_numbers.min
        # end
#
        # Finds a folder object by inventory path
        # Params:
        # +pool_name+:: the pool to find the folder for
        # +connection+:: the vcd connection object
        # returns a ManagedObjectReference for the folder found or nil if not found
        # def find_vm_folder(pool_name, connection)
          # Find a folder by its inventory path and return the object
          # Returns nil when the object found is not a folder
          # pool_configuration = pool_config(pool_name)
          # return nil if pool_configuration.nil?
#
          # folder = pool_configuration['folder']
          # datacenter = get_target_datacenter_from_config(pool_name)
          # return nil if datacenter.nil?
#
          # propSpecs = { # rubocop:disable Naming/VariableName
            # entity: self,
            # inventoryPath: "#{datacenter}/vm/#{folder}"
          # }
#
          # folder_object = connection.searchIndex.FindByInventoryPath(propSpecs) # rubocop:disable Naming/VariableName
          # return nil unless folder_object.instance_of? RbVmomi::VIM::Folder
#
          # folder_object
        # end

        # Returns an array containing cumulative CPU and memory utilization of a host, and its object reference
        # Params:
        # +model+:: CPU arch version to match on
        # +limit+:: Hard limit for CPU or memory utilization beyond which a host is excluded for deployments
        # returns nil if one on these conditions is true:
        #    the model param is defined and cannot be found
        #    the host is in maintenance mode
        #    the host status is not 'green'
        #    the cpu or memory utilization is bigger than the limit param
        # def get_host_utilization(host, model = nil, limit = 90)
          # limit = @config[:config]['utilization_limit'] if @config[:config].key?('utilization_limit')
          # return nil if model && !host_has_cpu_model?(host, model)
          # return nil if host.runtime.inMaintenanceMode
          # return nil unless host.overallStatus == 'green'
          # return nil unless host.configIssue.empty?
#
          # cpu_utilization = cpu_utilization_for host
          # memory_utilization = memory_utilization_for host
#
          # return nil if cpu_utilization.nil?
          # return nil if cpu_utilization.to_d == 0.0.to_d
          # return nil if memory_utilization.nil?
          # return nil if memory_utilization.to_d == 0.0.to_d
#
          # return nil if cpu_utilization > limit
          # return nil if memory_utilization > limit
#
          # [cpu_utilization, host]
        # end

        # def host_has_cpu_model?(host, model)
          # get_host_cpu_arch_version(host) == model
        # end

        # def get_host_cpu_arch_version(host)
          # cpu_model = host.hardware.cpuPkg[0].description
          # cpu_model_parts = cpu_model.split
          # cpu_model_parts[4]
        # end

        # def cpu_utilization_for(host)
          # cpu_usage = host.summary.quickStats.overallCpuUsage
          # return nil if cpu_usage.nil?
#
          # cpu_size = host.summary.hardware.cpuMhz * host.summary.hardware.numCpuCores
          # cpu_usage.fdiv(cpu_size) * 100
        # end

        # def memory_utilization_for(host)
          # memory_usage = host.summary.quickStats.overallMemoryUsage
          # return nil if memory_usage.nil?
#
          # memory_size = host.summary.hardware.memorySize / 1024 / 1024
          # memory_usage.fdiv(memory_size) * 100
        # end

        # def get_average_cluster_utilization(hosts)
          # utilization_counts = hosts.map { |host| host[0] }
          # utilization_counts.inject(:+) / hosts.count
        # end

        # def build_compatible_hosts_lists(hosts, percentage)
          # hosts_with_arch_versions = hosts.map do |h|
            # {
              # 'utilization' => h[0],
              # 'host_object' => h[1],
              # 'architecture' => get_host_cpu_arch_version(h[1])
            # }
          # end
          # versions = hosts_with_arch_versions.map { |host| host['architecture'] }.uniq
          # architectures = {}
          # versions.each do |version|
            # architectures[version] = []
          # end
#
          # hosts_with_arch_versions.each do |h|
            # architectures[h['architecture']] << [h['utilization'], h['host_object'], h['architecture']]
          # end
#
          # versions.each do |version|
            # targets = select_least_used_hosts(architectures[version], percentage)
            # architectures[version] = targets
          # end
          # architectures
        # end

        # def select_least_used_hosts(hosts, percentage)
          # raise('Provided hosts list to select_least_used_hosts is empty') if hosts.empty?
#
          # average_utilization = get_average_cluster_utilization(hosts)
          # least_used_hosts = []
          # hosts.each do |host|
            # least_used_hosts << host if host[0] <= average_utilization
          # end
          # hosts_to_select = (hosts.count * (percentage / 100.0)).to_int
          # hosts_to_select = hosts.count - 1 if percentage == 100
          # least_used_hosts.sort[0..hosts_to_select].map { |host| host[1].name }
        # end

        # def find_least_used_hosts(cluster, datacentername, percentage)
          # @connection_pool.with_metrics do |pool_object|
            # connection = ensured_vcd_connection(pool_object)
            # cluster_object = find_cluster(cluster, connection, datacentername)
            # raise("Cluster #{cluster} cannot be found") if cluster_object.nil?
#
            # target_hosts = get_cluster_host_utilization(cluster_object)
            # raise("there is no candidate in vcenter that meets all the required conditions, that the cluster has available hosts in a 'green' status, not in maintenance mode and not overloaded CPU and memory'") if target_hosts.empty?
#
            # architectures = build_compatible_hosts_lists(target_hosts, percentage)
            # least_used_hosts = select_least_used_hosts(target_hosts, percentage)
            # {
              # 'hosts' => least_used_hosts,
              # 'architectures' => architectures
            # }
          # end
        # end

        def find_host_by_dnsname(connection, dnsname)
          host_object = connection.searchIndex.FindByDnsName(dnsName: dnsname, vmSearch: false)
          return nil if host_object.nil?

          host_object
        end

        def find_least_used_host(cluster, connection, datacentername)
          logger.log('d', "Initialize find_least_used_host for cluster #{cluster} in datacenter #{datacentername}")
          target_hosts.min[1]
        end

        # def find_cluster(cluster, connection, datacentername)
          # datacenter = connection.serviceInstance.find_datacenter(datacentername)
          # raise("Datacenter #{datacentername} does not exist") if datacenter.nil?

#          # In the event the cluster is not a direct descendent of the
#          # datacenter, we use a ContainerView to leverage its recursive
#          # search. This will find clusters which are, for example, in
#          # folders under the datacenter. This will also find standalone
#          # hosts which are not part of a cluster.
          # cv = connection.serviceContent.viewManager.CreateContainerView(
            # container: datacenter.hostFolder,
            # type: ['ComputeResource', 'ClusterComputeResource'],
            # recursive: true
          # )
          # cluster = cv.view.find { |cluster_object| cluster_object.name == cluster }
          # cv.DestroyView
          # cluster
        # end

        # def get_cluster_host_utilization(cluster, model = nil)
          # cluster_hosts = []
          # cluster.host.each do |host|
            # host_usage = get_host_utilization(host, model)
            # cluster_hosts << host_usage if host_usage
          # end
          # cluster_hosts
        # end

        def find_least_used_vpshere_compatible_host(vm)
          logger.log('d', "Initialize find_least_used_vsphere_compatible_host for VM #{vm}")
          target_host = target_hosts.min[1]
          [target_host, target_host.name]
        end

        # def find_snapshot(vm, snapshotname)
          # get_snapshot_list(vm.snapshot.rootSnapshotList, snapshotname) if vm.snapshot
        # end

        # def build_propSpecs(datacenter, folder, vmname) # rubocop:disable Naming/MethodName
          # {
            # entity => self,
            # :inventoryPath => "#{datacenter}/vm/#{folder}/#{vmname}"
          # }
        # end

        # def find_vm(pool_name, vmname, connection)
          # Find a VM by its inventory path and return the VM object
          # Returns nil when a VM, or pool configuration, cannot be found
          # pool_configuration = pool_config(pool_name)
          # return nil if pool_configuration.nil?
#
          # folder = pool_configuration['folder']
          # datacenter = get_target_datacenter_from_config(pool_name)
          # return nil if datacenter.nil?
#
          # propSpecs = { # rubocop:disable Naming/VariableName
            # entity: self,
            # inventoryPath: "#{datacenter}/vm/#{folder}/#{vmname}"
          # }
#
          # connection.searchIndex.FindByInventoryPath(propSpecs) # rubocop:disable Naming/VariableName
        # end

        # def get_base_vm_container_from(connection)
          # view_manager = connection.serviceContent.viewManager
          # view_manager.CreateContainerView(
            # container: connection.serviceContent.rootFolder,
            # recursive: true,
            # type: ['VirtualMachine']
          # )
        # end

        # def get_snapshot_list(tree, snapshotname)
          # snapshot = nil
#
          # tree.each do |child|
            # if child.name == snapshotname
              # snapshot ||= child.snapshot
            # else
              # snapshot ||= get_snapshot_list(child.childSnapshotList, snapshotname)
            # end
          # end
#
          # snapshot
        # end

        # def get_vm_details(pool_name, vm_name, connection)
          # vm_object = find_vm(pool_name, vm_name, connection)
          # return nil if vm_object.nil?
#
          # parent_host_object = vm_object.summary.runtime.host if vm_object.summary&.runtime && vm_object.summary.runtime.host
          # raise('Unable to determine which host the VM is running on') if parent_host_object.nil?
#
          # parent_host = parent_host_object.name
          # architecture = get_host_cpu_arch_version(parent_host_object)
          # {
            # 'host_name' => parent_host,
            # 'object' => vm_object,
            # 'architecture' => architecture
          # }
        # end

        # def migration_enabled?(config)
          # migration_limit = config[:config]['migration_limit']
          # return false unless migration_limit.is_a? Integer
          # return true if migration_limit > 0
#
          # false
        # end

        # def migrate_vm(pool_name, vm_name)
          # @connection_pool.with_metrics do |pool_object|
            # begin
              # connection = ensured_vcd_connection(pool_object)
              # vm_hash = get_vm_details(pool_name, vm_name, connection)
#
              # raise StandardError, 'failed to get vm details. vm is unreachable or no longer exists' if vm_hash.nil?
#
              # @redis.with_metrics do |redis|
                # redis.hset("vmpooler__vm__#{vm_name}", 'host', vm_hash['host_name'])
                # migration_count = redis.scard('vmpooler__migration')
                # migration_limit = @config[:config]['migration_limit'] if @config[:config].key?('migration_limit')
                # if migration_enabled? @config
                  # if migration_count >= migration_limit
                    # logger.log('s', "[ ] [#{pool_name}] '#{vm_name}' is running on #{vm_hash['host_name']}. No migration will be evaluated since the migration_limit has been reached")
                    # break
                  # end
                  # run_select_hosts(pool_name, @provider_hosts)
                  # if vm_in_target?(pool_name, vm_hash['host_name'], vm_hash['architecture'], @provider_hosts)
                    # logger.log('s', "[ ] [#{pool_name}] No migration required for '#{vm_name}' running on #{vm_hash['host_name']}")
                  # else
                    # migrate_vm_to_new_host(pool_name, vm_name, vm_hash, connection)
                  # end
                # else
                  # logger.log('s', "[ ] [#{pool_name}] '#{vm_name}' is running on #{vm_hash['host_name']}")
                # end
              # end
            # rescue StandardError
              # logger.log('s', "[!] [#{pool_name}] '#{vm_name}' is running on #{vm_hash['host_name']}")
              # raise
            # end
          # end
        # end

        # def migrate_vm_to_new_host(pool_name, vm_name, vm_hash, connection)
          # @redis.with_metrics do |redis|
            # redis.sadd('vmpooler__migration', vm_name)
          # end
          # target_host_name = select_next_host(pool_name, @provider_hosts, vm_hash['architecture'])
          # # target_host_object = find_host_by_dnsname(connection, target_host_name)
          # # finish = migrate_vm_and_record_timing(pool_name, vm_name, vm_hash, target_host_object, target_host_name)
          # # @redis.with_metrics do |redis|
            # # redis.multi do |transaction|
              # # transaction.hset("vmpooler__vm__#{vm_name}", 'host', target_host_name)
              # # transaction.hset("vmpooler__vm__#{vm_name}", 'migrated', 'true')
            # end
          # end
          # logger.log('s', "[>] [#{pool_name}] '#{vm_name}' migrated from #{vm_hash['host_name']} to #{target_host_name} in #{finish} seconds")
        # ensure
          # @redis.with_metrics do |redis|
            # redis.srem('vmpooler__migration', vm_name)
          # end
        # end
#
        # def migrate_vm_and_record_timing(pool_name, vm_name, vm_hash, target_host_object, dest_host_name)
          # start = Time.now
          # migrate_vm_host(vm_hash['object'], target_host_object)
          # finish = format('%<time>.2f', time: Time.now - start)
          # metrics.timing("migrate.#{pool_name}", finish)
          # metrics.increment("migrate_from.#{vm_hash['host_name']}")
          # metrics.increment("migrate_to.#{dest_host_name}")
          # @redis.with_metrics do |redis|
            # checkout_to_migration = format('%<time>.2f', time: Time.now - Time.parse(redis.hget("vmpooler__vm__#{vm_name}", 'checkout')))
            # redis.multi do |transaction|
              # transaction.hset("vmpooler__vm__#{vm_name}", 'migration_time', finish)
              # transaction.hset("vmpooler__vm__#{vm_name}", 'checkout_to_migration', checkout_to_migration)
            # end
          # end
          # finish
        # end

        # def migrate_vm_host(vm_object, host)
          # relospec = RbVmomi::VIM.VirtualMachineRelocateSpec(host: host)
          # vm_object.RelocateVM_Task(spec: relospec).wait_for_completion
        # end

        # def create_folder(connection, new_folder, datacenter)
          # dc = connection.serviceInstance.find_datacenter(datacenter)
          # folder_object = dc.vmFolder.traverse(new_folder, RbVmomi::VIM::Folder, true)
          # raise("Cannot create folder #{new_folder}") if folder_object.nil?
#
          # folder_object
        # end

        # def find_template_vm(pool, connection)
          # datacenter = get_target_datacenter_from_config(pool['name'])
          # raise('cannot find datacenter') if datacenter.nil?
#
          # propSpecs = { # rubocop:disable Naming/VariableName
            # entity: self,
            # inventoryPath: "#{datacenter}/vm/#{pool['template']}"
          # }
#
          # template_vm_object = connection.searchIndex.FindByInventoryPath(propSpecs) # rubocop:disable Naming/VariableName
          # raise("Pool #{pool['name']} specifies a template VM of #{pool['template']} which does not exist for the provider #{name}") if template_vm_object.nil?
#
          # template_vm_object
        # end

        # def create_template_delta_disks(pool)
          # @connection_pool.with_metrics do |pool_object|
            # connection = ensured_vcd_connection(pool_object)
            # template_vm_object = find_template_vm(pool, connection)
#
            # template_vm_object.add_delta_disk_layer_on_all_disks
          # end
        # end

        # def valid_template_path?(template)
          # return false unless template.include?('/')
          # return false if template[0] == '/'
          # return false if template[-1] == '/'
#
          # true
        # end

        # def get_disk_backing(pool)
          # return :moveChildMostDiskBacking if linked_clone?(pool)
#
          # :moveAllDiskBackingsAndConsolidate
        # end

        # def linked_clone?(pool)
          # return if pool['create_linked_clone'] == false
          # return true if pool['create_linked_clone']
          # return true if @config[:config]['create_linked_clones']
        # end
      end
    end
  end
end
