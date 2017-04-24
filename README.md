# dbadmin

A Redis Enterprise Database administration tool

### Prerequisites
Python 2.7 or higher installed.

For windows, install pyreadline `pip install pyreadline`


### Install

Unzip the dbadmin.zip into some directory.

### Run

From the command line, run the command:  `python dbadmin.py <parameters>`

#### Command line parameters

`dbadmin.py [-h <host>] [-p <port>] -u <user name>`

Where:

host - Host name or IP address of the a node in the cluster. Default = "localhost"

port - The port number of the cluster. Default = 9443

user name - The cluster admin email address.

When running, you will be prompted for the admin password.


#### Usage

dbadmin supports the following operations:

`list [db|shards] [<db uid>|<db name>]`

`create <db name> [memory <memory size in GB>] [ram <RAM size in GB for flash>]`

`create <db name> json <json object>`

`change <db uid>|<db name> [shards <number of shards>] [replication true|false]
       [memory <memory size in GB>] [ram <RAM size in GB>] [replicaof add <db uid>|<db name>|<uri> |start|stop|off]`
	   
`change	<db uid>|<db name> json <json object>`
	   
`delete <db uid>|<db name>`

* list - Show databases or shards. If a db name or uid is specified, only this db or its shards will be shown.

* create - Create a new database specifying its name and optionaly, the maximum size in GB and the maximum RAM size for flash.
		   You can also create the database by sepcifying a json object.
		   
* change - Change configuration of a database specified by name or uid. You can make the following changes:
	* shards - Set the number of shards.
	* replication - Enable or disable replication.
	* memory - Maximum size in GB.
	* ram - RAM size in GB for flash.
	* replicaof - add a database or uri replicate from, start, stop or disable replica of.
	
	You can also change the database by sepcifying a json object.
	
* delete - Delete a database specified by name of uid.

