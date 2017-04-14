#!/usr/bin/python3

import sys, getopt, getpass, json

import requests
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pip.cmdoptions import src


has_tabulate = False
try:
    from tabulate import tabulate
    has_tabulate = True
except ImportError:
    has_tabulate = False
try:
    import readline
except ImportError:
    import pyreadline as readline

commands = ['create', 'change', 'delete', 'list', 'quit', 'help']
list_options = ['db', 'shards']
change_options = ['shards', 'replication', 'replicaof']
replication_options = ['true', 'false']
replicaof_options = ['add', 'off', 'start', 'stop']

def getfirst():
    idx = readline.get_begidx()
    full = readline.get_line_buffer()
    tokens = full[:idx].split()
    if len(tokens) > 0:
        return tokens[0]
    else:
        return ''
    
def getlast():
    idx = readline.get_begidx()
    full = readline.get_line_buffer()
    tokens = full[:idx].split()
    if len(tokens) > 0:
        return tokens[len(tokens) - 1]
    else:
        return ''

class SimpleCompleter(object):
    
    def __init__(self):
        self.command = ''
        self.subcommand = ''
        self.db = ''
        return

    def getOptions(self, text, options):
        if text:
            self.matches = [s for s in options if s and s.startswith(text)]
        else:
            self.matches = options[:]
            
    def getDBsOptions(self, text, ignore=''):
        dbs = getDBs(ignore)
        self.getOptions(text, dbs)

    def complete(self, text, state):
        response = None
        # This is the first time for this text, so build a match list.
        
        self.command = getfirst()
        last = getlast()
            
        if self.command == '':
            self.getOptions(text, commands)
        elif self.command == 'list':
            if last == self.command:
                self.getOptions(text, list_options)
            elif last in getDBs():
                self.matches = []
            else:
                self.getDBsOptions(text)
        elif self.command == 'delete':
            if last == self.command:
                self.getDBsOptions(text)
            else:
                self.matches = []
        elif self.command == 'change':
            if last == self.command:
                self.subcommand = ''
                self.getDBsOptions(text)
            elif last in getDBs():
                if self.subcommand == '':
                    self.db = last
                    self.subcommand = ''
                    self.getOptions(text, change_options)
                else:
                    self.matches = []
            elif last in change_options:
                self.subcommand = last
                if last == 'replication':
                    self.getOptions(text, replication_options)
                elif last == 'replicaof':
                    self.getOptions(text, replicaof_options)
                else:
                    self.matches = []
            elif last in replicaof_options:
                if last == 'add':
                    self.getDBsOptions(text, self.db)
                else:
                    self.matches = []
            else:
                self.matches = []
                    
        else:
            self.matches = []
        # Return the state'th item from the match list,
        # if we have that many.
        try:
            response = self.matches[state]
        except IndexError:
            response = None
        return response

readline.set_completer(SimpleCompleter().complete)
readline.parse_and_bind('tab: complete')
readline.parse_and_bind('set editing-mode vi')
readline.set_completer_delims(' \t\n')

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

exitCommands = ["EXIT", "QUIT", "BYE"]
db_headers = ['Uid', 'Name', 'Dns name', 'IP Address', 'Port', 'Shards', 'Replication']
shard_headers = ['Uid', 'DB Uid', 'Node Uid', 'Assigned Slots', 'Role']
yes = ["TRUE", "YES", "1", "ON"]
no = ["FALSE", "NO", "0", "OFF"]

GIGABYTE = 1024 * 1024 * 1024

host = 'localhost'
port = 9443
user = ''
passwd = ''
db_name_to_id = dict()

def get(param):
    url = "https://" + host + ":" + str(port) + "/v1/" + param 
    resp = requests.get(url, verify=False, auth=HTTPBasicAuth(user, passwd))
    if resp.status_code != requests.codes.ok:
        print("Error: " + resp.reason + ", " + str(resp.status_code))
        return None
    else:
        return resp.json()

def post(param, data):
    url = "https://" + host + ":" + str(port) + "/v1/" + param
    headers={'Content-Type': 'application/json'}
    resp = requests.post(url, data=data, headers=headers, verify=False, auth=HTTPBasicAuth(user, passwd))
    if resp.status_code != requests.codes.ok:
        print("Error: " + resp.reason + ", " + str(resp.status_code))
    else:
        print("OK")
        return resp.json()
   

def delete(param):
    url = "https://" + host + ":" + str(port) + "/v1/" + param 
    resp = requests.delete(url, verify=False, auth=HTTPBasicAuth(user, passwd))
    if resp.status_code != requests.codes.ok:
        print("Error: " + resp.reason + ", " + str(resp.status_code))
    else:
        print("OK")
        
def put(param, data):
    url = "https://" + host + ":" + str(port) + "/v1/" + param
    headers={'Content-Type': 'application/json'}
    resp = requests.put(url, data=data, headers=headers, verify=False, auth=HTTPBasicAuth(user, passwd))
    if resp.status_code != requests.codes.ok:
        print("Error: " + resp.reason + ", " + str(resp.status_code))
    else:
        print("OK")

def getDBs(ignore=''):
    names = []
    uids = []
    global db_name_to_id
    items = db_name_to_id.items()
    for i in items:
        if i[0] != ignore and str(i[1]) != ignore:
            names.append(i[0])
            uids.append(str(i[1]))
    return names + uids

def dbNameToUid():
    global db_name_to_id
    resp = get('bdbs')
    if resp != None:
        for db in resp:
            db_name_to_id[db["name"]] = db['uid']
        return True
    else:
        return False
 
def getDBUid(param):
    global db_name_to_id
    
    uid = -1
    try:
        uid = int(param)
    except ValueError:
        try:
            uid = db_name_to_id[param]      
        except KeyError:
            return -1
    
    return uid
    
def dbToRow(db):
    row = []
    row.append(str(db['uid']))
    row.append(db["name"])
    endpoints = db['endpoints']
    if len( endpoints) > 0: 
        row.append(endpoints[0]['dns_name'])
        if len(endpoints[0]['addr']) > 0:
            row.append(endpoints[0]['addr'][0])
        else:
            row.append('') 
        row.append(str(endpoints[0]['port']))
    else:
        row.append('')
        row.append('')
        row.append('')
    row.append(str(db['shards_count']))
    row.append(str(db['replication']))
    return row

def shardToRow(shard):
    row = []
    row.append(str(shard['uid']))
    row.append(str(shard["bdb_uid"]))
    row.append(str(shard["node_uid"]))
    row.append(shard['assigned_slots'])
    row.append(shard['role'])
    return row

def getReplicaOfUri(db):
    uri = ''
    resp = get('bdbs/' + str(db))
    if resp != None:
        uri = 'redis://admin:' + resp['authentication_admin_pass'] + '@' + resp['endpoints'][0]['dns_name'] + ':' + str(resp['endpoints'][0]['port'])

    return uri

def getReplicaOfList(db):
    resp = get('bdbs/' + db)
    if resp != None:
        return resp['sync_sources']
    else:
        return None
                
def getRepOf(dest, src):
    repof = getReplicaOfList(dest)
    uri = ''
    uid = getDBUid(src)
    if uid < 0:
        uri = src
    else:
        uri = getReplicaOfUri(uid)
    
    for r in repof:
        if r['uri'] == uri:
            print('Database is already a replica of this Uri')
            return ''
        
    repof.append({"uri": uri})
    
    return json.dumps(repof)

def printTable(rows, headers):
    if has_tabulate:
        print(tabulate(rows, headers, tablefmt='simple'))
    else:
        column_width = []
        i = 0
        for i in range(len(headers)):
            max = len(headers[i])
            for row in rows:
                if len(row[i]) > max:
                    max = len(row[i])
            column_width.append(max)
        
        for i in range(len(headers)):
            print(headers[i].ljust(column_width[i]+1), end=' ')
        print("")
        for i in range(len(headers)):
            print('-' * column_width[i], end='  ')
        print("")
        for row in rows:
            for i in range(len(headers)):
                print(row[i].ljust(column_width[i]+1), end=' ')
            print("")
        print("")
            
def listdb(uid):
    url = 'bdbs'
    rows = []
    if uid != '':
        url += '/' + uid
        resp = get(url)
        if resp != None:
            rows.append(dbToRow(resp))
            printTable(rows, db_headers)
            repof = resp['sync_sources']
            if len(repof) > 0:
                print('\nReplica of:')
                for r in repof:
                    print(r['uri'])
                print('Status: ' + resp['sync'])
    else:
        resp = get(url)
        if resp != None:
            for db in resp:
                rows.append(dbToRow(db))
            printTable(rows, db_headers)

def listshard(uid):
    rows = []
    url = ''
    if uid != '':
        url = 'bdbs/' + uid + '/shards'
    else:
        url = 'shards'
        
    resp = get(url)
    if resp != None:
        for shard in resp:
            rows.append(shardToRow(shard))
    
        printTable(rows, shard_headers)
                   
def exec_list(params):
    if len(params) == 0:
        print("Databases:")
        listdb('')
        print("\nShards:")
        listshard('')
        return
    
    entity = params[0]
    uid = ''
    if len(params) > 1:
        uid = getDBUid(params[1])
        if uid < 0:
            print("Database does not exist: " + params[1])
            return
    if entity == 'db':
        listdb(str(uid))
    elif entity == 'shards':
        listshard(str(uid))
    else:
        print('Invalid entity: ' + entity)          

def exec_create(params):
    global db_name_to_id
    
    if len(params) < 2:
        print('Missing parameters for create')
        return 
    name = params[0]
    memory_size = 0
    try:
        memory_size = int(params[1]) * GIGABYTE
    except ValueError:
        print('Illegal memory size: ' + params[1] + '. Must be a number')
        return
    
    if name in db_name_to_id:
        print('A database with this name already exist: ' + name)
        return
    
    data = '{ "name": "' + name + '", "type": "redis",  "memory_size": ' + str(memory_size) + ' }'
    resp = post('bdbs', data)
    if resp != '':
        uid = resp['uid']
        listdb(str(uid))
        db_name_to_id[resp["name"]] = resp['uid']
        

def exec_delete(params):
    global db_name_to_id
    
    if len(params) < 1:
        print('Missing parameters for delete')
        return
     
    uid = getDBUid(params[0])
    if uid < 0:
        print("Database does not exist: " + params[0])
        return
    
    delete('bdbs/' + str(uid))
    
    items = db_name_to_id.items()
    for i in items:
        if i[1] == uid:
            del db_name_to_id[i[0]]
            break

def exec_change(params):
    if len(params) < 1:
        print('Missing parameters for change')
        return
    
    uid = getDBUid(params[0])
    if uid < 0:
        print("Database does not exist: " + params[0])
        return
    
    params = params[1:]
    replication_changed = False
    replication = ''
    sharding = False
    replicaOf = False
    sync = ''
    repof = ''
    while len(params) > 0:
        p = params[0]
        if p == 'replication':
            replication_changed = True
            if len(params) < 2:
                print("Missing parameter for :" + p)
                return
            rep_param = params[1]
            if rep_param.upper() in yes:
                replication = 'true'
            elif rep_param.upper() in no:
                replication = 'false'
            else:
                print('Illegal parameter for replication: ' + rep_param)
                return
        elif p == 'shards':
            sharding = True
            if len(params) < 2:
                print("Missing parameter for :" + p)
                return
            try:
                shards = int(params[1])
            except ValueError:
                print('Illegal parameter for number of shards: ' + params[1] + '. Must be a number')
                return
        elif p == 'replicaof':
            if len(params) < 2:
                print("Missing parameter for :" + p)
                return
            
            action = params[1]
            if action == 'add':
                params = params[1:]
                sync = 'enabled'
                repof = getRepOf(str(uid), params[1])
            elif action == 'off':
                sync = 'disabled'
                repof = '[]'
            elif action == 'start':
                sync = 'enabled'
            elif action =='stop':
                sync = 'paused'
            else: 
                print ('Illegal replicaof action: ' + action)
                return
            
            replicaOf = True
        else:
            print('Invalid change action: ' + p)
            return
        params = params[2:]
    
    data = '{ '
    if replication_changed == True:
        data += '"replication": ' + replication
    if sharding == True:
        if replication_changed == True:
            data += ', '
        data += '"shard_key_regex": [ { "regex": ".*\\\{(?<tag>.*)\\\}.*" }, { "regex": "(?<tag>.*)" } ], "shards_count": ' + str(shards)
    if replicaOf == True:
        if replication_changed == True or replication_changed == True:
            data += ', '
        
        if sync != '':
            data += '"sync": "' + sync + '"'
        if repof != '':
            data += ', "sync_sources": ' + repof
    data += ' }'
    put('bdbs/' + str(uid), data)

def printHelp():
    print('list [db|shards [db uid]]')
    print('create <db name> <max size in GB>')
    print('change <db uid>|<db name> [shards <number of shards>] [replication true|false] [replicaof add <db uid>|<db name>|<uri> |start|stop|off]')
    print('delete <db uid>|<db name>')
    print()
    
def execCommand(params):
    command = params[0].upper()
    if command == 'LIST':
        exec_list(params[1:])
    elif command == 'CREATE':
        exec_create(params[1:])
    elif command == 'DELETE':
        exec_delete(params[1:])
    elif command == 'CHANGE':
        exec_change(params[1:])
    else:
        printHelp()
    
    
def main(argv):
    global host, port, user, passwd
    try:
        opts, args = getopt.getopt(argv, 'h:p:u:')
    except getopt.GetoptError:
        print("dbadmin [-h <host>] [-p <port>] -u <user name>")
        sys.exit(2)
    
    for opt, arg in opts:
        if opt == '-h':
            host = arg
        elif opt == '-p':
            try:
                port = int(arg)
            except ValueError:
                print('Illegal port number')
                sys.exit(2)
        elif opt == '-u':
            user = arg
        else:
            print("Invalid parameter " + opt)
            sys.exit(2)
    
    if user == '':
        print('Missing user name')
        sys.exit(2)
        
    passwd = getpass.getpass()
    
    if dbNameToUid() == False:
        print('Cannot connect to cluster')
        sys.exit(2)
    while True:
        command = input("dbadmin>")
        if command.upper() in exitCommands:
            break
        
        if command != '':
            execCommand(command.split())
            
    sys.stdout.flush()
    print("Goodbye")
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
