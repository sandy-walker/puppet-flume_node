
require 'tempfile'

Puppet::Type.newtype(:flume_node) do

  @doc = "Puppet type for configuring Flume nodes with the Master"

  newparam(:name) do
    desc "logical node name"
  end

  newparam(:source) do
    desc "source config"
  end

  newparam(:sink) do
    desc "sink config"
  end

  newparam(:master) do
    desc "master hostname"
  end

  newparam(:map_target) do
    desc "mapping target"
    defaultto {
      nil
    }
  end

  newproperty(:ensure) do
    desc "Whether the resource is in sync or not."

    defaultto :insync

    def retrieve
      `flume shell -q -c #{resource[:master]} -e getconfigs 2>/dev/null | grep #{resource[:name]} | grep -v null`
      ($? == 0 ? :insync : :outofsync)
    end

    newvalue :outofsync do
      master = resource[:master]
      name = resource[:name]
      unconf = <<-EOF
connect #{master}
exec unconfig #{name}
exec decommission #{name}
exec purge #{name}
EOF
      Tempfile.open("flume-") do |tempfile|
        tempfile.write(unconf)
        tempfile.close
        `cat #{tempfile.path} | flume shell -q 2>/dev/null`
      end
    end

    newvalue :insync do

      master = resource[:master]
      name = resource[:name]
      map_target = resource[:map_target]
      source = resource[:source]
      sink = resource[:sink]
      if sink.kind_of? Hash then
        # support hashes of the format:
        # { sinkType => [ array of nodes ] }
        # e.g.
        # { agentE2EChain => [ "flume1.example.com:35853", "flume2.example.com:35853" ] }
        sink = sink.keys.first + "( " + sink.values.first.shuffle.map{ |s| "\"#{s}\"" }.join(", ") + " )"
      end

      conf = <<-EOF
connect #{master}
exec unconfig #{name}
exec decommission #{name}
exec purge #{name}
exec config #{name} '#{source}' '#{sink}'
EOF
      unless map_target == nil
        mapping = <<-EOF
exec map #{map_target} #{name}
EOF
      end
      refresh = <<-EOF
exec refresh #{name}
EOF

      Tempfile.open("flume-") do |tempfile|
        tempfile.write(conf)
        if defined? mapping
          tempfile.write(mapping)
        end
        tempfile.write(refresh)
        tempfile.close
        `cat #{tempfile.path} | flume shell -q 2>/dev/null`
      end

    end

  end

end
