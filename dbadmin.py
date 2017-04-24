#!/usr/bin/python3

import sys, getopt, getpass, json, shlex

import requests
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning

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
        return tokens[-1]
    else:
        return ''

def gettokens():
    idx = readline.get_begidx()
    full = readline.get_line_buffer()
    tokens = full[:idx].split()
    return tokens

def getparmnum():
    return len(gettokens())

def getparam(idx):
    return gettokens()[idx]

class SimpleCompleter(object):
    
    def __init__(self, admin):
        self.dbadmin = admin
        self.command = ''
        self.db = ''
        return

    def getOptions(self, text, options, ignore=[]):
        options = [i for i in options if i not in ignore]
        if text:
            self.matches = [s for s in options if s and s.startswith(text)]
        else:
            self.matches = options[:]
            
    def getDBsOptions(self, text, ignore=''):
        dbs = self.dbadmin.getDBs(ignore)
        names = int(len(dbs) / 2)
        if text == '':
            self.getOptions(text, dbs[:names])
        else:
            self.getOptions(text, dbs)

    def complete(self, text, state):
        response = None
        # This is the first time for this text, so build a match list.
        
        self.command = getfirst()
        last = getlast()
            
        if self.command == '':
            self.getOptions(text, DBAdminShell.commands)
        elif self.command == 'list':
            if last == self.command:
                self.getOptions(text, DBAdmin.list_options)
            elif last in self.dbadmin.getDBs():
                self.matches = []
            else:
                self.getDBsOptions(text)
        elif self.command == 'create':
            n = getparmnum()
            if n == 2:
                self.getOptions(text, DBAdmin.create_options + ['json'])
            elif 'json' in gettokens():
                self.matches = []
            elif n % 2 == 0:
                self.getOptions(text, DBAdmin.create_options, gettokens()[2:])
            else:
                self.matches = []
        elif self.command == 'delete':
            if last == self.command:
                self.getDBsOptions(text)
            else:
                self.matches = []
        elif self.command == 'change':
            if last == self.command:
                self.getDBsOptions(text)
            elif last in self.dbadmin.getDBs():
                self.db = last
                self.getOptions(text, DBAdmin.change_options + ['json'])
            elif 'json' in gettokens()[2:]:
                self.matches = []
            elif last in DBAdmin.change_options:
                self.subcommand = last
                if last == 'replication':
                    self.getOptions(text, DBAdmin.replication_options)
                elif last == 'replicaof':
                    self.getOptions(text, DBAdmin.replicaof_options)
                else:
                    self.matches = []
            elif last in DBAdmin.replicaof_options:
                if last == 'add':
                    self.getDBsOptions(text, self.db)
                else:
                    self.matches = []
            elif self.db != '':
                self.getOptions(text, DBAdmin.change_options)
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

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class HttpConnector():

    def __init__(self, host, port, user, password):
        self.url = "https://" + host + ":" + str(port) + "/v1/"
        self.auth=HTTPBasicAuth(user, password)
    
    def get(self, param):
        url = self.url + param 
        resp = requests.get(url, verify=False, auth=self.auth)
        if resp.status_code != requests.codes.ok:
            print("Error: " + resp.reason + ", " + str(resp.status_code))
            return None
        else:
            return resp.json()

    def post(self, param, data):
        url = self.url + param
        headers={'Content-Type': 'application/json'}
        resp = requests.post(url, data=data, headers=headers, verify=False, auth=self.auth)
        if resp.status_code != requests.codes.ok:
            print("Error: " + resp.reason + ", " + str(resp.status_code))
        else:
            print("OK")
            return resp.json()
       
    
    def delete(self, param):
        url = self.url + param 
        resp = requests.delete(url, verify=False, auth=self.auth)
        if resp.status_code != requests.codes.ok:
            print("Error: " + resp.reason + ", " + str(resp.status_code))
        else:
            print("OK")
            
    def put(self, param, data):
        url = self.url + param
        headers={'Content-Type': 'application/json'}
        resp = requests.put(url, data=data, headers=headers, verify=False, auth=self.auth)
        if resp.status_code != requests.codes.ok:
            print("Error: " + resp.reason + ", " + str(resp.status_code))
        else:
            print("OK")

db_headers = ['Uid', 'Name', 'Dns name', 'IP Address', 'Port', 'Shards', 'Memory', 'Flags']
shard_headers = ['Uid', 'DB Uid', 'Node Uid', 'Assigned Slots', 'Role']
GIGABYTE = 1024 * 1024 * 1024
   
def dbToRow(db):
    row = []
    options = ''
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
    shards_count = db['shards_count']
    shards = str(shards_count)
    if db['replication'] == True:
        options += '(R)'
        shards += '(' + str(shards_count * 2) + ')'
    if shards_count > 1:
        options += '(C)'
    row.append(shards)
    memory = db['memory_size'] / GIGABYTE 
    memoryStr = "{0}".format(str(round(memory, 1) if memory % 1 else int(memory)))
    flash = db['bigstore']
    if flash == True:
        options += '(F)'
        ram = db['bigstore_ram_size'] / GIGABYTE
        memoryStr += '/(' + "{0}".format(str(round(ram, 1) if ram % 1 else int(ram))) + ')'
    row.append(memoryStr)
    repof = db['sync_sources']
    if len(repof) > 0:
        options += '(O)'
    row.append(options)
    return row

def shardToRow(shard):
    row = []
    row.append(str(shard['uid']))
    row.append(str(shard["bdb_uid"]))
    row.append(str(shard["node_uid"]))
    row.append(shard['assigned_slots'])
    row.append(shard['role'])
    return row
    
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
    

class DBAdmin():
    list_options = ['db', 'shards']
    create_options = ['ram', 'memory']
    change_options = ['shards', 'replication', 'replicaof', 'ram', 'memory']
    replication_options = ['true', 'false']
    replicaof_options = ['add', 'off', 'start', 'stop']
    yes = ["TRUE", "YES", "1", "ON"]
    no = ["FALSE", "NO", "0", "OFF"]
    
    def __init__(self, conn):
        self.conn = conn
        self.db_name_to_id = dict()
        
    def getDBs(self, ignore=''):
        names = []
        uids = []
        items = self.db_name_to_id.items()
        for i in items:
            if i[0] != ignore and str(i[1]) != ignore:
                names.append(i[0])
                uids.append(str(i[1]))
        return names + uids

    def dbNameToUid(self):
        self.db_name_to_id.clear()
        resp = self.conn.get('bdbs')
        if resp is not None:
            for db in resp:
                self.db_name_to_id[db["name"]] = db['uid']
            return True
        else:
            return False
 
    def getDBUid(self, param):
        uid = -1
        try:
            uid = int(param)
        except ValueError:
            try:
                uid = self.db_name_to_id[param]      
            except KeyError:
                return -1
        
        return uid
    
    def getMemorySize(self, db):
        resp = self.conn.get('bdbs/' + db)
        if resp is not None:
            return resp['memory_size']
        else:
            return 0
    
    def getReplicaOfUri(self, db):
        uri = ''
        resp = self.conn.get('bdbs/' + str(db))
        if resp is not None:
            uri = 'redis://admin:' + resp['authentication_admin_pass'] + '@' + resp['endpoints'][0]['dns_name'] + ':' + str(resp['endpoints'][0]['port'])
    
        return uri

    def getReplicaOfList(self, db):
        resp = self.conn.get('bdbs/' + db)
        if resp is not None:
            return resp['sync_sources']
        else:
            return None
                
    def getRepOf(self, dest, src):
        repof = self.getReplicaOfList(dest)
        uri = ''
        uid = self.getDBUid(src)
        if uid < 0:
            uri = src
        else:
            uri = self.getReplicaOfUri(uid)
        
        for r in repof:
            if r['uri'] == uri:
                print('Database is already a replica of this Uri')
                return ''
            
        repof.append({"uri": uri})
        return json.dumps(repof)
     
    def listdb(self, uid):
        url = 'bdbs'
        rows = []
        if uid != '':
            url += '/' + uid
            resp = self.conn.get(url)
            if resp is not None:
                rows.append(dbToRow(resp))
                printTable(rows, db_headers)
                repof = resp['sync_sources']
                if len(repof) > 0:
                    print('\nReplica of:')
                    for r in repof:
                        print(r['uri'])
                    print('Status: ' + resp['sync'])
        else:
            resp = self.conn.get(url)
            if resp is not None:
                for db in resp:
                    rows.append(dbToRow(db))
                printTable(rows, db_headers)

    def listshard(self, uid):
        rows = []
        url = ''
        if uid != '':
            url = 'bdbs/' + uid + '/shards'
        else:
            url = 'shards'
            
        resp = self.conn.get(url)
        if resp is not None:
            for shard in resp:
                rows.append(shardToRow(shard))
        
            printTable(rows, shard_headers)
                   
    def exec_list(self, params):
        if len(params) == 0:
            print("Databases:")
            self.listdb('')
            print("\nShards:")
            self.listshard('')
            return
        
        entity = params[0]
        uid = ''
        if len(params) > 1:
            uid = self.getDBUid(params[1])
            if uid < 0:
                print("Database does not exist: " + params[1])
                return
        if entity == 'db':
            self.listdb(str(uid))
        elif entity == 'shards':
            self.listshard(str(uid))
        else:
            print('Invalid entity: ' + entity)          

    def exec_create(self, params):
        if len(params) < 1:
            print('Missing database name')
            return 
        name = params[0]
        memory_size = 1 * GIGABYTE
        ram_size = 0
        flash = False
        
        params = params[1:]
        data = ''
        while len(params) > 0 and data == '':
            p = params[0]
            if len(params) < 2:
                print("Missing parameter for :" + p)
                return
            
            if p == 'memory':
                try:
                    memory_size = int(params[1]) * GIGABYTE
                except ValueError:
                    print('Illegal memory size: ' + params[1] + '. Must be a number')
                    return
            elif p == 'ram':
                try:
                    ram_size = int(params[1]) * GIGABYTE
                    flash = True
                except ValueError:
                    print('Illegal ram size: ' + params[1] + '. Must be a number')
                    return
            elif p == 'json':
                data = params[1]
            
            params = params[2:]
        
        if data == '':
            if ram_size > memory_size:
                print('Illegal RAM size: ' + str(ram_size) + '. Must less than total memory size: ' + str(memory_size))
                return
                
            if name in self.db_name_to_id:
                print('A database with this name already exist: ' + name)
                return
            
            data = '{ "name": "' + name + '", "type": "redis",  "memory_size": ' + str(memory_size) 
            if flash == True:
                data += ', "bigstore": true, "bigstore_ram_size": ' + str(ram_size) 
            data += ' }'
            
        resp = self.conn.post('bdbs', data)
        if resp is not None:
            uid = resp['uid']
            self.listdb(str(uid))
            self.db_name_to_id[resp["name"]] = resp['uid']
        

    def exec_delete(self, params):
        if len(params) < 1:
            print('Missing parameters for delete')
            return
         
        uid = self.getDBUid(params[0])
        if uid < 0:
            print("Database does not exist: " + params[0])
            return
        
        self.conn.delete('bdbs/' + str(uid))
        
        items = self.db_name_to_id.items()
        for i in items:
            if i[1] == uid:
                del self.db_name_to_id[i[0]]
                break

    def exec_change(self, params):
        if len(params) < 1:
            print('Missing parameters for change')
            return
        
        uid = self.getDBUid(params[0])
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
        flash = False
        memory = False
        memory_size = 0
        ram_size = 0
        data = ''
        while len(params) > 0 and data == '':
            p = params[0]
            if p == 'replication':
                replication_changed = True
                if len(params) < 2:
                    print("Missing parameter for :" + p)
                    return
                rep_param = params[1]
                if rep_param.upper() in DBAdmin.yes:
                    replication = 'true'
                elif rep_param.upper() in DBAdmin.no:
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
                    repof = self.getRepOf(str(uid), params[1])
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
            elif p == 'ram':
                if len(params) < 2:
                    print('Missing RAM size for flash')
                    return
            
                try:
                    ram_size = int(params[1]) * GIGABYTE
                    flash = True
                except ValueError:
                    print('Illegal RAM size: ' + params[1] + '. Must be a number')
                    return
            elif p == 'memory':
                if len(params) < 2:
                    print('Missing memory size')
                    return
                try:
                    memory_size = int(params[1]) * GIGABYTE
                    memory = True
                except ValueError:
                    print('Illegal memory size: ' + params[1] + '. Must be a number')
                    return
            elif p == 'json':
                data = params[1]         
            else:
                print('Invalid change action: ' + p)
                return
            params = params[2:]
        
        if data == '':
            if memory_size == 0:
                memory_size = self.getMemorySize(str(uid))
            if ram_size > memory_size:
                print('Illegal RAM size: ' + params[1] + '. Must less than total memory size: ' + str(memory_size / GIGABYTE))
                return
                
            data = '{ '
            if replication_changed == True:
                data += '"replication": ' + replication
            if sharding == True:
                data += ', "shard_key_regex": [ { "regex": ".*\\\{(?<tag>.*)\\\}.*" }, { "regex": "(?<tag>.*)" } ], "shards_count": ' + str(shards)
            if replicaOf == True:
                if sync != '':
                    data += ', "sync": "' + sync + '"'
                if repof != '':
                    data += ', "sync_sources": ' + repof
            if memory == True:
                data += ', "memory_size": ' + str(memory_size) 
            if flash == True:
                data += ', "bigstore": true, "bigstore_ram_size": ' + str(ram_size) 
            data += ' }'
            if data[2] == ',':
                data = data[:2] + data[3:]
                
        self.conn.put('bdbs/' + str(uid), data)

class DBAdminShell:
    exitCommands = ["EXIT", "QUIT", "BYE"]
    commands = ['create', 'change', 'delete', 'list', 'quit', 'help']

    def __init__(self, admin):
        self.admin = admin

    def printHelp(self):
        print('list [db|shards] [<db uid>|<db name>]')
        print('create <db name> [memory <memory size in GB>] [ram <RAM size in GB for flash>]')
        print('create <db name> json <json object>')
        print('change <db uid>|<db name> [shards <number of shards>] [replication true|false]')
        print('       [memory <memory size in GB>] [ram <RAM size in GB for flash>] [replicaof add <db uid>|<db name>|<uri> |start|stop|off]')
        print('change <db uid>|<db name> json <json object>')
        print('delete <db uid>|<db name>')
        print()
    
    def execCommand(self, params):
        command = params[0].upper()
        if command == 'LIST':
            self.admin.exec_list(params[1:])
        elif command == 'CREATE':
            self.admin.exec_create(params[1:])
        elif command == 'DELETE':
            self.admin.exec_delete(params[1:])
        elif command == 'CHANGE':
            self.admin.exec_change(params[1:])
        else:
            self.printHelp()
    
    def run(self):
        if self.admin.dbNameToUid() == False:
            print('Cannot connect to cluster')
            return
        
        while True:
            command = input("dbadmin>")
            if command.upper() in DBAdminShell.exitCommands:
                break
            
            if command != '':
                self.execCommand(shlex.split(command))
            
    
def main(argv):
    host = 'localhost'
    port = 9443
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
    
    httpConnection = HttpConnector(host, port, user, passwd)
    admin = DBAdmin(httpConnection)
    readline.set_completer(SimpleCompleter(admin).complete)
    readline.parse_and_bind('tab: complete')
    readline.parse_and_bind('set editing-mode vi')
    readline.set_completer_delims(' \t\n')
    
    
    DBAdminShell(admin).run()
    print("Goodbye")
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
