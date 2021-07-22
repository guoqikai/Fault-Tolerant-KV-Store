# Fault Tolerant Distributed Key Value Store Service

A 1-fault tolerant key value store service implemented using C.
## Design:
The service consists of three executable components: client, server, and coordinator. Users can use the client to put or get a key value pair from the service.
### Client
When client executes its first put/get operation, it will request server configurations from the coordinator, which includes addresses/ports of all running servers. To make sure we can distribute data evenly, the client will hash the key using MD5, and the target server id will be `hashed_value % num_servers`. Since we have a fixed number of running servers, same key will always give the same sever id. When put/get request fails (it happens if the target server is down), the client will request server configurations from the coordinator again.
### Server
Each server has two hash tables: primary table and secondary table. When the client sends a PUT request to a server (call it `server_a`), the server will store the value in its primary table and forward the request to its secondary server (call it `server_b`, then `server_b`'s primary server is `server_a`), which will store the value in its secondary table. Notice that because each server needs to connect to its primary server and secondary server, the service needs to have at least three servers + the coordinator.

### Coordinator
Coordinator is the core of this service. Its job includes managing server topology, monitoring servers health by listening to heartbeat messages sent from the servers, and leading the recovery process. Coordinator will start the recovery process if it does not receive a heartbeat message from a server (call it `server_a`) for a time.  The process consists of 1+5 steps:
* step 0: update server configuration so that request to `server_a` will be sent to its secondary server (call it `server_b`), send request to `server_b` so that it can handle client request to its secondary table (i.e. keys belong to `server_a`).
* step 1: kill `server_a`, clean up, spawn a new server (call it `server_a+`).
* step 2: send update requests to the `server_a`'s primary and secondary server with the address of `server_a+`. These two servers will send `server_a`'s data to `server_a+`.
* step 3, 4: receive update completed request from these servers.
* step 5: Send SET-SECONDARY request to `server_a+` so `server_a+` will connect to `server_b` and after this point the server can function like a normal server. Then, send  request to `server_b` so it will no longer handle client requests to keys that belong to `server_a`. Failure in any of these steps will lead to a roll back to step 1. If the coordinator detects another dead server during the recovery step, it will
  signal shutdown requests to all servers.

## How to use:
1. cd to root dir of the project, run `make all`
2. Edit the config file if needed, the default config file will spawn three servers on localhost. First line of the config file is number of servers and rest lines are configurations of each server. The format is: `<host_name>` `<clients_port>` `<servers_port>` `<coord_port>`  To spawn server on remote machine, change `<host_name>` to a ssh url. If any of the port is set to zero, a random available port will be used
3. Start the coordinator: `./coord -c <coord_port> -s <servers_port> -C config.txt -l log.txt`, replace `<cooord_port>` and `<servers_port>` with a port number
4. Open a new terminal and start a client: `./client -h <host> -p <coord_port>`. If you are using the provided config file, then `<host>` is `localhost`
5. To put a value, use `P "<key>" "value"`. To get a value, use `G "<key>"`
6. Open a new terminal and run `ps -a`, you should see a list of servers. Use `kill <pid>` to kill a server and observe how the service reacts on this
7. To terminate the service, use `ctrl+d` in the coordinator terminal
8. Each server will dump the two hash tables into files called `server_<server_id>.primary` and `server_<server_id>.secondary` when the service terminates. You can take a look at them and log files if you're interested

