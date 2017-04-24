"""
Ref: http://stackoverflow.com/questions/27834815/how-to-run-multiple-instances-of-python-plugin-in-collectd
Ref: https://github.com/signalfx/collectd-mongodb/blob/master/mongodb.py
"""
import redis
import collectd

class Redis(object):


    def __init__(self):
        self.plugin_name = "redis_slowlog"
        self.host = "127.0.0.1"
        self.port = 6379
        self.slowlog_counter_last = 0

    def submit(self, type, value, type_instance=None):
        v = collectd.Values(plugin='redis_slowlog')
        v.type = type
        v.type_instance = type_instance
        v.values = [value, ]
        v.dispatch()

    def get_redis_conn(self):
        return redis.StrictRedis(host=self.host, port=self.port)

    def fetch_slowlog_len(self, conn):
        return conn.slowlog_len()

    def fetch_slowlog_duration(self, conn, num):
        max_duration = 0
        if num > 0:
            for i in conn.slowlog_get(num):
                if i['duration']  > max_duration:
                    max_duration = i['duration']
        return max_duration

    def read(self):
        conn = self.get_redis_conn()

        slowlog_len = self.fetch_slowlog_len(conn)

        if slowlog_len < self.slowlog_counter_last:
            slowlog_counter = slowlog_len
        else:
            slowlog_counter = slowlog_len - self.slowlog_counter_last

        self.slowlog_counter_last = slowlog_len
        self.submit('counter', slowlog_counter, 'slowlog_count')

        slowlog_duration = self.fetch_slowlog_duration(conn, slowlog_counter)
        self.submit('gauge', slowlog_duration, 'slowlog_duration')

    def config(self, obj):
        for node in obj.children:
            key = node.key.lower()
            val = node.values[0]
            if node.key == 'Host':
                self.host = node.values[0]
            elif node.key == 'Port':
                self.port = int(node.values[0])
            else:
                collectd.warning('redis_slowlog plugin: Unknown config key: %s' % node.key)

rd = Redis()
collectd.register_config(rd.config)
collectd.register_read(rd.read)
