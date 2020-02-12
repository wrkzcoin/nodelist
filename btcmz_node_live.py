#!/usr/bin/python3.7
from typing import List, Dict
import asyncio, aiohttp
from aiohttp import web
import time, json
import pymysql.cursors
# redis
import redis
# For some environment variables
import os, sys, traceback

DBHOST = os.getenv('NODEBTCMZ_MYSQL_HOST', 'localhost')
DBUSER = os.getenv('NODEBTCMZ_MYSQL_USER', 'user')
DBNAME = os.getenv('NODEBTCMZ_MYSQL_NAME', 'dbname')
DBPASS = os.getenv('NODEBTCMZ_MYSQL_PASS', 'dbpassword')
        
REMOTE_NODES_URL = "https://raw.githubusercontent.com/bitcoinmono/bitcoinmono-nodes-json/master/bitcoinmono-nodes.json"
SLEEP_CHECK = 10  # 10s
NODE_LIVE_LIST = []
REMOTE_NODES_JSON = None

conn = None
redis_pool = None
redis_conn = None
COIN = "BTCMZ"

def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, db=36)

# Open Connection
def openConnection():
    global conn
    try:
        if conn is None:
            conn = pymysql.connect(DBHOST, user=DBUSER, passwd=DBPASS, db=DBNAME, charset='utf8', 
                cursorclass=pymysql.cursors.DictCursor, connect_timeout=5)
        elif (not conn.open):
            conn = pymysql.connect(DBHOST, user=DBUSER, passwd=DBPASS, db=DBNAME, charset='utf8mb4', 
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=5)    
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()


def insert_nodes(nodelist):
    global conn, COIN
    openConnection()
    try:
        with conn.cursor() as cursor:
            sql = """ INSERT INTO `pubnodes_"""+COIN.lower()+"""` (`name`, `url`, `port`, `url_port`, `ssl`, 
                      `cache`, `fee_address`, `fee_fee`, `online`, `version`, `timestamp`, `getinfo_dump`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            for each in nodelist:
                list = tuple([value for k, value in each.items()])
                cursor.execute(sql, (list))
            conn.commit()
    finally:
        conn.close()


# Start the work
async def getNodeList():
    global REMOTE_NODES_JSON
    time_out = 12
    async with aiohttp.ClientSession() as session:
        async with session.get(REMOTE_NODES_URL, timeout=time_out) as response:
            try:
                resp = await response.json()
            except Exception as e:
                resp = json.loads(await response.read())
    data = resp['nodes']
    REMOTE_NODES_JSON = data
    node_list = []  # array of nodes
    proto = 'http://'
    for node in data:
        getinfo = None
        getfee = None
        try:
            if node['ssl'] == True:
                proto = 'https://'
            else:
                proto = 'http://'
            node_url = proto + node['url'].strip()+':'+str(node['port'])+'/info'
            print("Checking {}".format(node_url))
            async with aiohttp.ClientSession() as session:
                async with session.get(node_url, timeout=time_out) as response:
                    try:
                        getinfo = await response.json()
                    except Exception as e:
                        getinfo = json.loads(await response.read())
                    try:
                        node_url = proto + node['url'].strip()+':'+str(node['port'])+'/fee'
                        print("Checking {}".format(node_url))
                        async with aiohttp.ClientSession() as session:
                            async with session.get(node_url, timeout=time_out) as response:
                                try:
                                    getfee = await response.json()
                                except Exception as e:
                                    getfee = json.loads(await response.read())
                    except asyncio.TimeoutError:
                        print('TIMEOUT: {}'.format(node_url))
                        pass
        except asyncio.TimeoutError:
            print('TIMEOUT: {}'.format(node_url))
            pass
        except Exception as e:
            print(e)
            pass
        if getinfo and getfee:
            node_list.append({
                'name': node['name'],
                'url': node['url'].strip(),
                'port': node['port'],
                'url_port': node['url'].strip().lower() + ':' + str(node['port']),
                'ssl': 1 if 'ssl' in node and node['ssl'] == True else 0,
                'cache': 1 if 'cache' in node and node['cache'] == True else 0,
                'fee_address': getfee['address'] if 'address' in getfee and len(getfee['address']) == 99 else "",
                'fee_fee': int(getfee['amount']) if ('amount' in getfee) and (len(str(getfee['amount'])) < 10) and (len(getfee['address']) == 99) else 0,,
                'online': 1,
                'version': getinfo['version'],
                'timestamp': int(time.time()),
                'getinfo_dump': json.dumps(getinfo)
                })
        else:
            node_list.append({
                'name': node['name'],
                'url': node['url'].strip(),
                'port': node['port'],
                'url_port': node['url'].strip().lower() + ':' + str(node['port']),
                'ssl': 1 if 'ssl' in node and node['ssl'] == True else 0,
                'cache': 1 if 'cache' in node and node['cache'] == True else 0,
                'fee_address': "",
                'fee_fee': 0,
                'online': 0,
                'version': "",
                'timestamp': 0,
                'getinfo_dump': ''
                })
    return node_list


async def handle_get_nodelist(request):
    global conn, REMOTE_NODES_URL, REMOTE_NODES_JSON, redis_pool, redis_conn, COIN
    response_obj = None
    response_dump = None
    if redis_conn is None:
        try:
            redis_conn = redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    if redis_conn.exists(f'{COIN}_NODELIVE'):
        response_dump = redis_conn.get(f'{COIN}_NODELIVE')
        expired_time = json.loads(response_dump)[0]
        if int(time.time()) - expired_time < 60:
            # if redis data is less than 60s.
            response_obj = json.loads(json.loads(response_dump)[1])
            return web.json_response(response_obj, status=200)
    time_out = 5
    node_list = []
    if REMOTE_NODES_JSON is None:
        async with aiohttp.ClientSession() as session:
            async with session.get(REMOTE_NODES_URL, timeout=time_out) as response:
                try:
                    resp = await response.json()
                except Exception as e:
                    resp = json.loads(await response.read())
        REMOTE_NODES_JSON = resp['nodes']  
    openConnection()
    try:
        with conn.cursor() as cursor:
            for each in REMOTE_NODES_JSON:
                node = each['url'].strip().lower() + ':' + str(each['port'])
                sql = """ SELECT SUM(`online`) FROM (SELECT `pubnodes_"""+COIN.lower()+"""`.`online`, `pubnodes_"""+COIN.lower()+"""`.`timestamp` 
                          FROM `pubnodes_"""+COIN.lower()+"""` WHERE `pubnodes_"""+COIN.lower()+"""`.`url_port` = %s AND `pubnodes_"""+COIN.lower()+"""`.`timestamp`> """ + str(int(time.time()-10800)) + """
                          ORDER BY `pubnodes_"""+COIN.lower()+"""`.`timestamp` DESC LIMIT 100) AS `availability` """
                cursor.execute(sql, (node))
                node_avail = cursor.fetchone()
                availablity = int(node_avail['SUM(`online`)'] if node_avail['SUM(`online`)'] else 0) or 0
                if availablity > 0:
                    sql = """ SELECT `name`, `url`, `port`, `ssl`, `cache`, `fee_address`, `fee_fee`, `online`, `version`, `timestamp`
                              FROM `pubnodes_"""+COIN.lower()+"""` WHERE `pubnodes_"""+COIN.lower()+"""`.`url_port` = %s 
                              ORDER BY `pubnodes_"""+COIN.lower()+"""`.`timestamp` DESC LIMIT 1 """
                    cursor.execute(sql, (node))
                    node_data = cursor.fetchone()
                    if node_data:
                        node_list.append({
                            'name': node_data['name'],
                            'url': node_data['url'],
                            'port': node_data['port'],
                            'ssl': True if node_data['ssl'] == 1 else False,
                            'cache': True if node_data['cache'] == 1 else False,
                            'fee': {'address': node_data['fee_address'], 'amount': node_data['fee_fee']},
                            'availability': availablity,
                            'online': True if node_data['online'] == 1 else False,
                            'version': node_data['version'],
                            'timestamp': node_data['timestamp']
                        })
    finally:
        conn.close()
    response_obj = {"nodes": node_list}
    response_dump = [int(time.time()), json.dumps(response_obj)]
    try:
        redis_conn.set(f'{COIN}_NODELIVE', json.dumps(response_dump), ex=90)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    # json_string = json.dumps(response_obj).replace(" ", "")
    return web.json_response(response_obj, status=200)


# node_check_bg
async def node_check_bg(app):
    global NODE_LIVE_LIST, COIN
    tmp_node = []
    while True:
        try:
            try:
                tmp_node = await getNodeList()
                insert_nodes(tmp_node)
            except Exception as e:
                print(e)
            if len(tmp_node) > 0:
                print("==============")
                print("Get total nodes {}.".format(len(tmp_node)))
                print("==============")
            else:
                print('Currently 0 nodes... Sleep {s}'.format(SLEEP_CHECK))
        except asyncio.CancelledError:
            pass
        print("Sleep {}s".format(SLEEP_CHECK))
        await asyncio.sleep(SLEEP_CHECK)


async def start_background_tasks(app):
    app['get_node_live'] = asyncio.create_task(node_check_bg(app))


async def cleanup_background_tasks(app):
    app['get_node_live'].cancel()
    await app['get_node_live']


app = web.Application()
app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)

app.router.add_route('GET', '/list', handle_get_nodelist)

web.run_app(app, host='127.0.0.1', port=8084)
