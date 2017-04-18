"""
Ref: http://stackoverflow.com/questions/27834815/how-to-run-multiple-instances-of-python-plugin-in-collectd
"""
import redis
import collectd

CONFIGS = []

def config_callback(conf):
  """Receive configuration block"""
  for node in conf.children:
    key = node.key.lower()
    val = node.values[0]
    if key == 'host':
      host = val
    elif key == 'port':
      port = int(val)
    else:
      collectd.warning('redis_slowlog plugin: Unknown config key: %s' % key)
      continue

  CONFIGS.append({
    'host': host,
    'port': port,
  })

def read_callback():
  """Read a key from info response data and dispatch a value"""
  for conf in CONFIGS:
    r = redis.StrictRedis(host=conf['host'], port=conf['port'])
    try:
      value = r.slowlog_get(1)[0]['duration']
    except:
      value = 'null'
    val = collectd.Values(plugin='redis_slowlog')
    val.type = 'gauge'
    val.type_instance = 'slowlog'
    val.values = [int(value)]
    val.dispatch()

collectd.register_config(config_callback)
collectd.register_read(read_callback)
