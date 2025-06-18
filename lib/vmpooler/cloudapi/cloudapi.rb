class CloudAPI
  def self.cloudapi_login(vcloud_url,auth_encoded,api_version)
    logger.log('d', "[#{name}] Connection Pool - authenticating to vCD #{vcloud_url} with API version #{api_version} auth_coded #{auth_encoded}")
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
      logger.log('d', "[#{name}] Connection Pool - succesfull authenticated to vCD #{vcloud_url} with API version #{api_version}")
      connection = {
        vcloud_url: vcloud_url,
        api_version: api_version,
        auth_encoded: auth_encoded,
        session_token: response['X-VMWARE-VCLOUD-ACCESS-TOKEN'],
      }
      connection
    else
      logger.log('d', "[#{name}] Connection Pool - authentication failed to vCD #{vcloud_url} with API version #{api_version}")
      nil
    end
  end
  def self.cloudapi_check_session(connection)
    logger.log('d', "CJS Check cloudapi_sessions #{connection[:vcloud_url]} is still active")
    uri = URI("#{connection[:vcloud_url]}/cloudapi/1.0.0/sessions/current")
    request = Net::HTTP::Get.new(uri)
    request['Accept'] = "application/*;version=#{connection[:api_version]}"
    request['Authorization'] = "Bearer #{connection[:session_token]}"
    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
      http.request(request)
    end
    if response.is_a?(Net::HTTPSuccess)
      logger.log('d', "CJS cloudapi_sessions still active")
      true
    else
      logger.log('d', "CJS cloudapi_session NOT ACTIVE")
      false
    end
  end
  def self.cloudapi_vapp(pool, connection)
    vapp_name = pool['vapp']
    vapp_name = pool['name'] if vapp_name.nil? || vapp_name.empty?
    logger.log('d', "[CJS] Checking vapp #{vapp_name} in vdc")
    query_url = "#{connection[:vcloud_url]}/api/query?type=vApp&format=records&filter=name==#{vapp_name}"
    uri = URI(query_url)
    vapp_response = Net::HTTP.get_response(uri, headers)
    if vapp_response.code.to_i == 200
      logger.log('d', "[CJS] vapp #{vapp_name} already exists in vdc")
      vapp = {name: vapp_name, network: pool['network']}
    else
      # create vapp
      logger.log('d', "[CJS] Creating vapp #{vapp_name} in vdc")
      uri = URI("#{connection[:vcloud_url]}/action/composeVApp")
      request = Net::HTTP::Post.new(uri)
      request['Accept'] = headers['Accept']
      request['Authorization'] = headers['Authorization']
      xml_body = <<~XML
        <ComposeVAppParams
            xmlns="http://www.vmware.com/vcloud/v1.5"
            name="#{vapp_name}"
            deploy="true"
            powerOn="false">
          <Description>"vap for vmpooler pool #{vapp_name}"</Description>
          <InstantiationParams>
            <!-- Optional: Add network or other instantiation parameters here -->
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
        logger.log('d', "CJS VApp '#{vapp_name}' created successfully.")
        vapp = {name: vapp_name, network: pool['network']}
      else
        logger.log('d', "CJS Failed to create VApp: #{response.code} #{response.message}")
        vapp = nil
      end
    end
  end
end