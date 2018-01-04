require File.expand_path(File.join(File.dirname(__FILE__), '..', 'mongodb'))
Puppet::Type.type(:mongodb_shard).provide(:mongo, parent: Puppet::Provider::Mongodb) do
  desc 'Manage mongodb sharding.'

  confine true:     begin
      require 'json'
      true
    rescue LoadError
      false
    end

  mk_resource_methods

  commands mongo: 'mongo'

  def self.mongod_conf_file
    file = '/etc/mongodb-shard.conf'
    file
  end

  def initialize(value = {})
    super(value)
    @property_flush = {}
  end

  def destroy
    @property_flush[:ensure] = :absent
  end

  def create
    @property_flush[:ensure] = :present
    @property_flush[:member] = resource.should(:member)
    @property_flush[:keys]   = resource.should(:keys)
  end

  def sh_addshard(member)
    mongo_command("sh.addShard(\"#{member}\")", '127.0.0.1:27017')
  end

  def sh_shardcollection(shard_key)
    collection = shard_key.keys.first
    keys = shard_key.values.first.map do |key, value|
      "\"#{key}\": #{value}"
    end

    mongo_command("sh.shardCollection(\"#{collection}\", {#{keys.join(',')}})", '127.0.0.1:27017')
  end

  def sh_enablesharding(member)
    mongo_command("sh.enableSharding(\"#{member}\")", '127.0.0.1:27017')
  end

  def self.prefetch(resources)
    instances.each do |prov|
      resource = resources[prov.name]
      resource.provider = prov if resource
    end
  end

  def flush
    set_member
    @property_hash = self.class.shard_properties(resource[:name])
  end

  def set_member
    if @property_flush[:ensure] == :absent
      # a shard can't be removed easily at this time
      return
    end

    return unless @property_flush[:ensure] == :present && @property_hash[:ensure] != :present

    Puppet.debug "Adding the shard #{name}"
    output = sh_addshard(@property_flush[:member])
    raise Puppet::Error, "sh.addShard() failed for shard #{name}: #{output['errmsg']}" if output['ok'].zero?
    output = sh_enablesharding(name)
    raise Puppet::Error, "sh.enableSharding() failed for shard #{name}: #{output['errmsg']}" if output['ok'].zero?

    return unless @property_flush[:keys]

    @property_flush[:keys].each do |key|
      output = sh_shardcollection(key)
      raise Puppet::Error, "sh.shardCollection() failed for shard #{name}: #{output['errmsg']}" if output['ok'].zero?
    end
  end

  def self.instances
    shards_properties.map do |shard|
      new shard
    end
  end

  def self.shard_collection_details(obj, shard_name)
    collection_array = []
    obj.each do |database|
      next unless database['_id'].eql?(shard_name) && !database['shards'].nil?
      collection_array = database['shards'].map do |collection|
        { collection.keys.first => collection.values.first['shardkey'] }
      end
    end
    collection_array
  end

  def self.shard_properties(shard)
    properties = {}
    output = mongo_command('sh.status()', '127.0.0.1:27017')
    output['shards'].each do |s|
      next unless s['_id'] == shard
      properties = {
        name: s['_id'],
        ensure: :present,
        member: s['host'],
        keys: shard_collection_details(output['databases'], s['_id']),
        provider: :mongo
      }
    end
    properties
  end

  def self.shards_properties
    output = mongo_command('sh.status()', '127.0.0.1:27017')
    properties = if !output['shards'].empty?
                   output['shards'].map do |shard|
                     {
                       name: shard['_id'],
                       ensure: :present,
                       member: shard['host'],
                       keys: shard_collection_details(output['databases'], shard['_id']),
                       provider: :mongo
                     }
                   end
                 else
                   []
                 end
    Puppet.debug("MongoDB shard properties: #{properties.inspect}")
    properties
  end

  def exists?
    @property_hash[:ensure] == :present
  end

  def mongo_command(command, host, retries = 4)
    self.class.mongo_command(command, host, retries)
  end

  def self.mongo_command(command, host = nil, retries = 4)
    # Allow waiting for mongod to become ready
    # Wait for 2 seconds initially and double the delay at each retry
    wait = 2
    begin
      output = mongo_eval(command, 'admin', retries, host)
    rescue Puppet::ExecutionFailure => e
      raise unless e =~ %r{Error: couldn't connect to server} && wait <= (2**max_wait)

      info("Waiting #{wait} seconds for mongod to become available")
      sleep wait
      wait *= 2
      retry
    end

    output_hash = nil

    # NOTE (spredzy) : sh.status()
    # does not return a json stream
    # we jsonify it so it is easier
    # to parse and deal with it
    if command == 'sh.status()'
      sh_status_lines = output.split("\n")

      # Remove leading junk "--- Sharding Status ---"
      sh_status_lines.shift

      # Our output hash.
      output_hash = {
        "sharding version" => {},
        "shards" => [],
        "databases" => [],
      }

      # The current and previous lines.
      line = nil
      prev_line = nil

      # The section currently being parsed.
      section = nil
      # Sections we don't want to ignore.
      valid_sections = [ 'sharding version', 'shards', 'databases' ]
      # In an ignored section.
      in_ignore = false

      while ! sh_status_lines.empty?
        line = sh_status_lines.shift

        if line =~ %r{^\S} || line.empty?
          next
        end

        if line =~ %r{^ {2}\S}
          # Found a top-level section.

          # Some top-level sections (sharding version) end in a closing brace.
          next if line == "  }"

          # Obtain the section name.
          section = line.gsub(%r{^  ([^:]+):.*$}, '\1')

          next
        end

        case section
        when 'sharding version'
          line.gsub!(%r{,$}, '')
          kv_match = line.match(%r{^\s*"([^"]+)"\s*:\s*(.*)$})
          key = kv_match[1]
          value = kv_match[2]

          if value =~ %r{^\d+$}
            value = value.to_i
          else
            value.gsub!(%r{^"(.*)"$}, '\1')
          end

          output_hash[section][key] = value
        when 'shards'
          output_hash[section] << JSON.parse(line)
        when 'databases'
          next if line =~ %r{^\s*[^\{[:space:]]|-->>}
          output_hash[section] << JSON.parse(line)
        end
      end
    else
      # Hack to avoid non-json empty sets
      output = '{}' if output == "null\n"
      output.gsub!(%r{\s*}, '')
      output.gsub!(%r{[[:alpha:]]+\([^)]*\)}, 'null')
      output_hash = JSON.parse(output)
    end

    output_hash
  end
end
