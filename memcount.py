import time
import datetime
import redis


delay = 0.5
server_addrs = ({'ip': "10.77.96.122", 'port': 33336},{'ip': "10.77.96.122", 'port': 33337})
count_prefix = 'counter_'
total_prefix = '_total'
servers = []
result_map = {}
clear_daily_flag = set()

def display(servers, key, result_key):
    print '-----------------------print once------------------'
    for server in servers:
        display_map = server.hgetall(key)
        print 'print every'
        for k, v in display_map.iteritems():
            print k, v
        display_map = server.hgetall(result_key)
        print 'print total'
        for k, v in display_map.iteritems():
            print k, v


for server_addr in server_addrs:
    servers.append(redis.Redis(host=server_addr['ip'], port=server_addr['port']))

while True:
    now = datetime.datetime.now()
    daily = now.strftime('%Y%m%d')

    cur_hour = now.hour 
    if cur_hour == 15:
        yestoday = now.date() - datetime.timedelta(days=1)
        old_daily = yestoday.strftime('%Y%m%d')

        oldestday= now.date() - datetime.timedelta(days=2)
        oldest_daily = oldestday.strftime('%Y%m%d')

        if old_daily not in  clear_daily_flag:
            old_key = count_prefix + old_daily
            old_total_key = count_prefix + old_daily + total_prefix
            for server in servers:
                server.expire(old_key, 1000)
                server.expire(old_total_key, 1000)
            clear_daily_flag.add(old_daily)

        if oldest_daily in clear_daily_flag:
            clear_daily_flag.remove(oldest_daily)
    

    key = count_prefix + daily

    for server in servers:
        count_map = server.hgetall(key)
        for k, v in count_map.iteritems():
            if k in result_map:
                result_map[k] = int(v) + result_map[k]
            else:
                result_map[k] = int(v)

    for k, v in result_map.iteritems(): 
                result_map[k] = str(v)

    result_key = key + total_prefix

    if len(result_map) > 0: 
        for server in servers:
            server.hmset(result_key, result_map)
    display(servers, key, result_key)
    time.sleep(delay)
    result_map.clear()
