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

`dbadmin [-h <host>] [-p <port>] -u <user name> [-w password] [command]`

Where:

host - Host name or IP address of the a node in the cluster. Default = "localhost"

port - The port number of the cluster. Default = 9443

user name - The cluster admin email address.

Password - an optional admin password. When not provided, you will be prompted for the admin password.

command - an optional command to run. If provided, the command will be executed and exit. If not provided, you will enter interactive mode.

#### Usage

dbadmin supports the following operations:

`list [db|shards] [<db uid>|<db name>]`

`create <db name> [memory <memory size in GB>] [ram <RAM size in GB for flash>] [port <port number>] [replication] [rack]
		[persist <persistence method>] [eviction <eviction policy>] [dbpass <database password>]`
		
`create <db name> json <json object>`

`change <db uid>|<db name> [shards <number of shards>] [replication true|false] [rack true|false]
       [memory <memory size in GB>] [ram <RAM size in GB>] [replicaof add <db uid>|<db name>|<uri> |start|stop|off]`
	   [persist <persistence method>] [eviction <eviction policy>] [dbpass <database password>]
	   
`change	<db uid>|<db name> json <json object>`
	   
`delete <db uid>|<db name>`

* list - Show databases or shards. If a db name or uid is specified, only this db or its shards will be shown.

* create - Create a new database specifying its name and optionaly, the maximum size in GB, the maximum RAM size for flash,
		   the port number and whether to enable replication and rack zone awareness (if supported by the cluster).
		   You can also specify persistence, evicetion policy and set a database password.
		   Alternatively, you can also create the database by sepcifying a json object.
		   
* change - Change configuration of a database specified by name or uid. You can make the following changes:
	* shards - Set the number of shards.
	* replication - Enable or disable replication.
	* rack - Enable or disable rack zone awareness (if supported by the cluster). 
	* memory - Maximum size in GB.
	* ram - RAM size in GB for flash.
	* replicaof - add a database or uri replicate from, start, stop or disable replica of.
	* persist - Set or change persistence.
	* eviction - Change eviction policy.
	* dbpass - Change database password
	
	You can also change the database by sepcifying a json object.
	
* delete - Delete a database specified by name of uid.

