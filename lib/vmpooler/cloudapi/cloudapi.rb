class Logger
  def self.log(level, message)
    time = Time.now.strftime('%Y-%m-%d %H:%M:%S')
    puts "\e[32m[#{time}] [CloudAPI] #{message}\e[0m" if ENV['VMPOOLER_DEBUG']
  end
end
class CloudAPI
  def self.cloudapi_login(vcloud_url,auth_encoded,api_version)
    uri = URI("#{vcloud_url}/cloudapi/1.0.0/sessions")
    request = Net::HTTP::Post.new(uri)
    request['Accept'] = "application/*;version=#{api_version}"
    # Create Base64 encoded authorization string
    #auth_string = Base64.strict_encode64("#{username}:#{password}")
    #puts auth_string
    request['Authorization'] = "Basic #{auth_encoded}"
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
      http.request(request)
    end
    if response.is_a?(Net::HTTPSuccess)
      Logger.log('d', "[#{name}] Connection Pool - succesfull authenticated to vCD #{vcloud_url} with API version #{api_version}")
      session_token = response['X-VMWARE-VCLOUD-ACCESS-TOKEN']
      connection = {
        vcloud_url: vcloud_url,
        api_version: api_version,
        auth_encoded: auth_encoded,
        session_token: response['X-VMWARE-VCLOUD-ACCESS-TOKEN']
      }
      connection
    else
      Logger.log('d', "[#{name}] Connection Pool - authentication failed to vCD #{vcloud_url} with API version #{api_version}")
      nil
    end
  end
  def self.cloudapi_check_session(connection)
    Logger.log('d', "[CJS] Check cloudapi_sessions #{connection[:vcloud_url]} is still active")
    uri = URI("#{connection[:vcloud_url]}/cloudapi/1.0.0/sessions/current")
    request = Net::HTTP::Get.new(uri)
    request['Accept'] = "application/*;version=#{connection[:api_version]}"
    request['Authorization'] = "Bearer #{connection[:session_token]}"
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
      http.request(request)
    end
    if response.is_a?(Net::HTTPSuccess)
      Logger.log('d', "[CJS] cloudapi_sessions still active")
      true
    else
      Logger.log('d', "[CJS] cloudapi_session NOT ACTIVE")
      false
    end
  end
  def self.check_vapp_exists(vapp_name, connection)
    query_url = "#{connection[:vcloud_url]}/api/query?type=vApp&format=records&filter=name==#{vapp_name}"
    uri = URI(query_url)
    headers = {
      'Accept' => "application/*+json;version=#{connection[:api_version]}",
      'Authorization' => "Bearer #{connection[:session_token]}"
    }
    Net::HTTP.get_response(uri, headers)
  end
  def self.cloudapi_vapp(pool, connection)
    vapp_name = pool['vapp']
    vapp_name = pool['name'] if vapp_name.nil? || vapp_name.empty?
    Logger.log('d', "[CJS] Checking vapp #{vapp_name} in vdc")

    vapp_response = check_vapp_exists(vapp_name, connection)
    vapp_response_body = JSON.parse(vapp_response.body)

    if vapp_response.code.to_i == 200 and vapp_response_body['total'].to_i == 1
      Logger.log('d', "[CJS] vapp #{vapp_name} already exists in vdc")
      vapp = {
        name: vapp_response_body['record'][0]['name'],
        href: vapp_response_body['record'][0]['href']
      }
      return vapp
    else
      # create vapp
      Logger.log('d', "[CJS] Creating vapp #{vapp_name} in vdc")
      uri = URI("#{connection[:vdc_href]}/action/composeVApp")
      Logger.log('d', "[CJS]  #{connection[:vdc_href]}/action/composeVApp in vdc")
      request = Net::HTTP::Post.new(uri)
      request['Accept'] = "application/*+json;version=#{connection[:api_version]}"
      request['Authorization'] = "Bearer #{connection[:session_token]}"
      xml_body = <<~XML
        <ComposeVAppParams
            xmlns="http://www.vmware.com/vcloud/v1.5"
            name="#{vapp_name}"
            deploy="true"
            powerOn="true">
          <Description>"VApp for vmpooler pool #{vapp_name}"</Description>
          <InstantiationParams>
            <!-- Optional: Add network or other instantiation parameters here -->
            <NetworkConfigSection>
              <ovf:Info xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1">VApp info</ovf:Info>
              <NetworkConfig networkName="#{pool['network']}">
                <Configuration>
                  <IpScopes>
                    <IpScope>
                      <IsInherited>true</IsInherited>
                      <Gateway>10.77.179.1</Gateway>
                      <SubnetPrefixLength>25</SubnetPrefixLength>
                      <Dns1/>
                      <Dns2/>
                      <DnsSuffix/>
                      <IsEnabled>true</IsEnabled>
                    </IpScope>
                  </IpScopes>
                  <ParentNetwork href="https://t01-s01-vcd01.s01.t01.1p.kpn.com/api/admin/network/dc44b058-be5a-4343-8de2-a640ea74d972"/>
                  <FenceMode>bridged</FenceMode>
                  <AdvancedNetworkingEnabled>true</AdvancedNetworkingEnabled>
                </Configuration>
                <IsDeployed>false</IsDeployed>
              </NetworkConfig>
            </NetworkConfigSection>
          </InstantiationParams>
          <!-- Add more <SourcedItem> blocks for additional VMs or templates -->
          <AllEULAsAccepted>true</AllEULAsAccepted>
        </ComposeVAppParams>
      XML
      request.body = xml_body
      request.content_type = 'application/vnd.vmware.vcloud.composeVAppParams+xml'
      response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
        http.request(request)
      end
      if response.is_a?(Net::HTTPSuccess)
        Logger.log('d', "[CJS] VApp '#{vapp_name}' created successfully.")
        vapp_response = check_vapp_exists(vapp_name, connection)
        vapp_response_body = JSON.parse(vapp_response.body)
        if vapp_response.code.to_i == 200 and vapp_response_body['total'].to_i == 1
          vapp = {
            name: vapp_response_body['record'][0]['name'],
            href: vapp_response_body['record'][0]['href']
          }
          return vapp
        else
          Logger.log('d', "[CJS] Failed to retrieve vApp details after creation.")
          nil
        end
      else
        Logger.log('d', "[CJS] Failed to create VApp: #{response.code} #{response.message}")
        nil
      end
    end
  end
  def self.get_vms_in_pool(connection, pool)
    vms = []
    vapp = cloudapi_vapp(pool, connection)
    headers = {
      'Accept' => "application/vnd.vmware.vcloud.vm+json;version=#{connection[:api_version]}",
      'Authorization' => "Bearer #{connection[:session_token]}"
    }
    uri = URI(vapp[:href])
    vapp_response = Net::HTTP.get_response(uri, headers)

    if vapp_response.code.to_i != 200
      puts_red "Failed to retrieve vApp: #{vapp_response.body}"
      return []
    end
    #puts "vap_response: #{vapp_response.code} #{vapp_response.message}  #{vapp_response.body}"
    root = Nokogiri::XML(vapp_response.body)

    namespace = 'http://www.vmware.com/vcloud/v1.5'
    vm_elements = root.xpath('//vcloud:Vm', 'vcloud' => namespace)
    vm_elements.map { |vm| { 'name' => vm['name'] } }
  end

  def self.get_vm(vm_name, connection, pool)
    vm_hash = {}
    query_url = "#{connection[:vcloud_url]}/api/query?type=vm&format=records&filter=name==#{vm_name};containerName==#{pool['vapp']}"
    uri = URI(query_url)
    headers = {
      'Accept' => "application/*+json;version=#{connection[:api_version]}",
      'Authorization' => "Bearer #{connection[:session_token]}"
    }
    vm_response = Net::HTTP.get_response(uri, headers)
    vm_response_body = JSON.parse(vm_response.body) if vm_response.is_a?(Net::HTTPSuccess)
    #puts "[CJS] VM response: #{vm_response.code} #{vm_response.message}\n#{vm_response.body}"
    if vm_response.is_a?(Net::HTTPSuccess) and vm_response_body['total'].to_i == 1
      vm_response_body['record'][0].each do |key, value|
        vm_hash[key] = value
      end
    else
      Logger.log('d', "[CJS] VM '#{vm_name}' not found or multiple VMs with the same name exist.")
    end
    return vm_hash
  end
  def self.check_vm_exists(vm_name, connection, pool)
    query_url = "#{connection[:vcloud_url]}/api/query?type=vm&format=records&filter=name==#{vm_name};containerName==#{pool['vapp']}"
    uri = URI(query_url)
    headers = {
      'Accept' => "application/*+json;version=#{connection[:api_version]}",
      'Authorization' => "Bearer #{connection[:session_token]}"
    }
    Net::HTTP.get_response(uri, headers)
  end
  def self.cloudapi_get_catalog_item_href(pool, connection)
    href = nil
    catalog_query_url = "#{connection[:vcloud_url]}/api/query?type=vm&pageSize=1000&sortDesc=name&filter=isVAppTemplate==true;status!=FAILED_CREATION;status!=UNKNOWN;status!=UNRECOGNIZED;status!=UNRESOLVED&links=true"
    headers = {
      'Accept' => "application/*+json;version=#{connection[:api_version]}",
      'Authorization' => "Bearer #{connection[:session_token]}"
    }
    uri = URI(catalog_query_url)
    response = Net::HTTP.get_response(uri, headers)
    if response.is_a?(Net::HTTPSuccess)
      data = JSON.parse(response.body)
      vm_templates = data['record']
      vm_templates.each do |template|
        if template['containerName'] == pool['template'] and template['catalogName'] == pool['catalog']
          href = template['href']
        end
      end
    end
    if href.nil?
      Logger.log('d', "[CJS] Cannot find VM Template: #{pool['template']} - Catalog: #{pool['catalog']}")
    else
      Logger.log('d', "[CJS] Found VM Template: #{pool['template']} - Catalog: #{pool['catalog']} with href: #{href}")
    end
    return href
  end
  def self.cloudapi_get_storage_policy_href(pool, connection)
    href = nil
    storage_query_url = "#{connection[:vcloud_url]}/api/query?type=orgVdcStorageProfile&filter=vdc==#{connection[:vdc_id]}&format=records"
    headers = {
      'Accept' => "application/*+json;version=#{connection[:api_version]}",
      'Authorization' => "Bearer #{connection[:session_token]}"
    }
    uri = URI(storage_query_url)
    response = Net::HTTP.get_response(uri, headers)
    if response.is_a?(Net::HTTPSuccess)
      data = JSON.parse(response.body)
      storage_policy = data['record']
      storage_policy.each do |policy|
        if policy['name'] == pool['storage_policy']
          href = policy['href']
        end
      end
    end
    if href.nil?
      Logger.log('d', "[CJS] Cannot find Storage policy: #{pool['storage_policy']}")
    end
    return href
  end
  def self.destroy_vm(vm_hash, connection)
    poweroff_vm(vm_hash, connection) if vm_hash['status'] == 'POWERED_ON'
    puts "[CVM] VM Href: #{vm_hash['href']}"
    Logger.log('d', "[CVM] Deleting VM #{vm_hash['href']}")
    uri = URI("#{vm_hash['href']}")
    request = Net::HTTP::Delete.new(uri)
    request['Accept'] = "application/*+json;version=#{connection[:api_version]}"
    request['Authorization'] = "Bearer #{connection[:session_token]}"
    request.content_type = 'application/vnd.vmware.vcloud.task+xml'
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
      http.request(request)
    end
    return response
  end
  def self.poweron_vm(vm_hash, connection)
    Logger.log('d', "[CVM] Powering on VM '#{vm_hash['href']}'")
    uri = URI("#{vm_hash['href']}/power/action/powerOn")
    request = Net::HTTP::Post.new(uri)
    request['Accept'] = "application/*+json;version=#{connection[:api_version]}"
    request['Authorization'] = "Bearer #{connection[:session_token]}"
    request.content_type = 'application/vnd.vmware.vcloud.powerOnParams+xml'
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
      http.request(request)
    end
    return response
  end
  def self.poweroff_vm(vm_hash, connection)
    Logger.log('d', "[CVM] Powering off VM '#{vm_hash['href']}'")
    uri = URI("#{vm_hash['href']}/power/action/powerOff")
    request = Net::HTTP::Post.new(uri)
    request['Accept'] = "application/*+json;version=#{connection[:api_version]}"
    request['Authorization'] = "Bearer #{connection[:session_token]}"
    request.content_type = 'application/vnd.vmware.vcloud.powerOffParams+xml'
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
      http.request(request)
    end
    return response
  end
  def self.cloudapi_create_vm(new_vmname, pool, connection, vapp)
    vm_hash = {}
    Logger.log('d', "[CVM] Creating VM '#{new_vmname}' in vApp '#{pool['vapp']}'  href: '#{vapp[:href]}'")
    # Check if the VM already exists
    vm_hash = get_vm(new_vmname, connection, pool)
    if !vm_hash.empty?
      puts "[CJS] VM #{new_vmname} already exists in vApp '#{pool['vapp']}'"
    else
      puts "[CJS] VM #{new_vmname} does not exist, proceeding to create it in vApp '#{pool['vapp']}'."
      # --------------------------------------------------------------------------------------------------
      # Check if the storage policy exists and get its href
      os_drive_storage_tier_href = cloudapi_get_storage_policy_href(pool, connection)
      catalogItem_href = cloudapi_get_catalog_item_href(pool, connection)
      # Prepare the XML body for the VM creation request
      # IpAddressAllocationMode can be one of the following (as per VMware vCloud API):
      # - "POOL": IP is allocated from the network's IP pool
      # - "DHCP": IP is allocated via DHCP
      # - "MANUAL": IP is manually specified
      # - "NONE": No IP allocation
      #
      # Example usage:
      # <root:IpAddressAllocationMode>DHCP</root:IpAddressAllocationMode>
      xml_body = <<~XML
        <root:RecomposeVAppParams xmlns:root="http://www.vmware.com/vcloud/v1.5" xmlns:ns0="http://schemas.dmtf.org/ovf/envelope/1">
          <root:SourcedItem>
          <root:Source href="#{catalogItem_href}"
          name="#{new_vmname}"
          type="application/vnd.vmware.vcloud.vm+xml"/>
          <root:InstantiationParams>
             <root:NetworkConnectionSection><ns0:Info/>
          <root:PrimaryNetworkConnectionIndex>0</root:PrimaryNetworkConnectionIndex>
          <root:NetworkConnection network="#{pool['network']}">
            <root:NetworkConnectionIndex>0</root:NetworkConnectionIndex>
            <root:IpAddress/>
            <root:IpType>IPV4</root:IpType>
            <root:IsConnected>true</root:IsConnected>
            <root:IpAddressAllocationMode>POOL</root:IpAddressAllocationMode>
            <root:NetworkAdapterType>VMXNET3</root:NetworkAdapterType>
          </root:NetworkConnection>
            </root:NetworkConnectionSection>
          </root:InstantiationParams>
        <root:StorageProfile href="#{os_drive_storage_tier_href}" type="application/vnd.vmware.vcloud.vdcStorageProfile+xml"/>
          </root:SourcedItem>
          <root:AllEULAsAccepted>true</root:AllEULAsAccepted>
        </root:RecomposeVAppParams>
      XML
      # Compose the API endpoint for VM instantiation
      #uri = URI("https://t01-s01-vcd01.s01.t01.1p.kpn.com/api/vApp/vapp-aaa3ce71-4acb-4a7f-b203-143184fba23c/action/recomposeVApp")
      uri = URI("#{vapp[:href]}/action/recomposeVApp")
      request = Net::HTTP::Post.new(uri)
      request['Accept'] = "application/*+json;version=#{connection[:api_version]}"
      request['Authorization'] = "Bearer #{connection[:session_token]}"
      request.content_type = 'application/vnd.vmware.vcloud.recomposeVAppParams+xml'
      request.body = xml_body
      response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
        http.request(request)
      end
      if response.is_a?(Net::HTTPSuccess)
        Logger.log('d', "[CVM] VM '#{new_vmname}' created successfully in vApp '#{pool['vapp']}'")
      else
        Logger.log('d', "[CVM] Failed to create VM: #{response.code} #{response.message}")
      end
    end
  end
end
