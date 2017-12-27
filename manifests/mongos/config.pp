# PRIVATE CLASS: do not call directly
class mongodb::mongos::config (
  $ensure          = $mongodb::mongos::ensure,
  $config          = $mongodb::mongos::config,
  $config_content  = $mongodb::mongos::config_content,
  $config_template = $mongodb::mongos::config_template,
  $configdb        = $mongodb::mongos::configdb,
  $config_data     = $mongodb::mongos::config_data,
  $ssl             = $mongodb::mongos::ssl,
  $ssl_key         = $mongodb::mongos::ssl_key,
  $ssl_ca          = $mongodb::mongos::ssl_ca,
  $ssl_weak_cert   = $mongodb::mongos::ssl_weak_cert,
  $ssl_invalid_hostnames = $mongodb::mongos::ssl_invalid_hostnames
) {

  if ($ensure == 'present' or $ensure == true) {

    #Pick which config content to use
    if $config_content {
      $config_content_real = $config_content
    } elsif $config_template {
      # Template has $config_data hash available
      $config_content_real = template($config_template)
    } else {
      # Template has $config_data hash available
      $config_content_real = template('mongodb/mongodb-shard.conf.erb')
    }

    file { $config:
      content => $config_content_real,
      owner   => 'root',
      group   => 'root',
      mode    => '0644',
    }

  }

}
