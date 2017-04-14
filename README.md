# dbadmin

A Redis Enterprise Database administration tool

Prerequisites: Python 2.7 or higher installed.

### Install

Unzip the dbadmin.zip into some directory.

### Run

From the command line, run the command:  `python dbadmin.py <parameters>`

#### Command line parameters

`dbadmin.py [-h \<host\>] [-p \<port\>] -u \<user name\>`

Where:

host - Host name or IP address of the a node in the cluster. Default = "localhost"

port - The port number of the cluster. Default = 9443

user name - The cluster admin email address.

When running, you will be prompted for the admin password.


#### Usage

dbadmin supports the following operations:

`list [db|shards [db uid]]`
`create \<db name\> \<max size in GB\>`
`change \<db uid\>|\<db name\> [shards \<number of shards\>] [replication true|false] [replicaof add \<db uid\>|\<db name\>|\<uri\> |start|stop|off]`
`delete \<db uid\>|\<db name\>`

* list - Show databases or shards. If a db name or uid is specified, only this db or its shards will be shown.
* create - Create a new database specifying its name and the maximum size in GB.
* change - Change configuration of a database specified by name or uid. You can make the following changes:
	* shards - Set the number of shards.
	* replication - Enable or disable replication.
	* replicaof - add a database or uri replicate from, start, stop or disable replica of.
* delete - Delete a database specified by name of uid.

