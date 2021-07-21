// The key-value server implementation

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "defs.h"
#include "hash.h"
#include "util.h"

// Program arguments
#define HEARTBEAT_INTERVAL 0.3

// Host name and port number of the metadata server
static char coord_host_name[HOST_NAME_MAX] = "";
static uint16_t coord_port = 0;

// Ports for listening to incoming connections from clients, servers and coord
static uint16_t clients_port = 0;
static uint16_t servers_port = 0;
static uint16_t coord_in_port = 0;

// Current server id and total number of servers
static int server_id = -1;
static int num_servers = 0;

// Log file name
static char log_file_name[PATH_MAX] = "";

static void usage(char **argv)
{
    printf(
        "usage: %s -h <coord host> -m <coord port> -c <clients port> "
        "-s <servers port> -M <coord incoming port> -S <server id> "
        "-n <num servers> [-l <log file>]\n",
        argv[0]);
    printf(
        "If the log file (-l) is not specified, log output is written "
        "to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
    char option;
    while ((option = getopt(argc, argv, "h:m:c:s:M:S:n:l:")) != -1)
    {
        switch (option)
        {
        case 'h':
            strncpy(coord_host_name, optarg, HOST_NAME_MAX);
            break;
        case 'm':
            coord_port = atoi(optarg);
            break;
        case 'c':
            clients_port = atoi(optarg);
            break;
        case 's':
            servers_port = atoi(optarg);
            break;
        case 'M':
            coord_in_port = atoi(optarg);
            break;
        case 'S':
            server_id = atoi(optarg);
            break;
        case 'n':
            num_servers = atoi(optarg);
            break;
        case 'l':
            strncpy(log_file_name, optarg, PATH_MAX);
            break;
        default:
            fprintf(stderr, "Invalid option: -%c\n", option);
            return false;
        }
    }

    // Allow server to choose own ports. Uncomment extra conditions if
    // server ports must be specified on command line.
    return (coord_host_name[0] != '\0') && (coord_port != 0) &&
           //(clients_port != 0) && (servers_port != 0) &&
           //(coord_in_port != 0) &&
           (num_servers >= 3) && (server_id >= 0) && (server_id < num_servers);
}

// Socket for sending requests to the coordinator
static int coord_fd_out = -1;
// Socket for receiving requests from the coordinator
static int coord_fd_in = -1;

// Sockets for listening for incoming connections from clients, servers
// and coordinator
static int my_clients_fd = -1;
static int my_servers_fd = -1;
static int my_coord_fd = -1;

// Store fds for all connected clients, up to MAX_CLIENT_SESSIONS
static int client_fd_table[MAX_CLIENT_SESSIONS];

// Store fds for connected servers
#define MAX_SERVER_SESSIONS 2
static int server_fd_table[MAX_SERVER_SESSIONS] = {-1, -1};

// Storage for this server's primary key set
hash_table primary_hash = {0};

// Primary server (the one that stores the primary copy for this server's
// secondary key set)
static int primary_sid = -1;
static int primary_fd = -1;

// Storage for this server's secondary key set
hash_table secondary_hash = {0};

// Secondary server (the one that stores the secondary copy for this server's
// primary key set)
static int secondary_sid = -1;
static int secondary_fd = -1;

static void cleanup();

static const int hash_size = 65536;

typedef enum
{
    NORMAL,
    PRIMARY_RECOVERY,
    SECONDARY_RECOVERY,
    SHUT_DOWN
} server_state_t;

#define MAX_THREAD 4

// index of threads:
// 	main thread: 0
//	server listener: 1
//	hearbeat thread: 2
//  recovery thread: 3
static server_state_t global_server_state = NORMAL;
static server_state_t thread_server_states[MAX_THREAD] = {0};
static bool server_state_dirty[MAX_THREAD] = {false};
pthread_mutex_t server_state_lock = PTHREAD_MUTEX_INITIALIZER;

static pthread_t hearbeat_thread;
static pthread_t server_listener_thread;
// since f=1, we can have at most one recovery_thread;
static pthread_t recovery_thread;

server_state_t update_server_state(int thread_ind) {
    pthread_mutex_lock(&server_state_lock);
    thread_server_states[thread_ind] = global_server_state;
    server_state_dirty[thread_ind] = false;
    pthread_mutex_unlock(&server_state_lock);
    return thread_server_states[thread_ind];
}

server_state_t get_server_state(int thread_ind) {
    return thread_server_states[thread_ind];
}

// set thread's own state and global state to server_state. Mark all
// other thread's state dirty bit.
// if server_state_drity[thread_index] is true when this function is called,
// global server state  will be set to SHUT_DOWN
void set_server_state(server_state_t server_state, int thread_ind) {
    pthread_mutex_lock(&server_state_lock);
	if (server_state_dirty[thread_ind]) {
		log_error("State conflict detected!\n");
		server_state = SHUT_DOWN;
	} 
	global_server_state = server_state;
	thread_server_states[thread_ind] = server_state;

    memset(server_state_dirty, true, sizeof(bool) * MAX_THREAD);
    server_state_dirty[thread_ind] = false;
    pthread_mutex_unlock(&server_state_lock);
}

void *run_heartbeat(void *args) {
    (void)args;
    while (update_server_state(2) != SHUT_DOWN)
    {
        char send_buffer[MAX_MSG_LEN] = {0};
        coord_ctrl_request *req = (coord_ctrl_request *)send_buffer;
        req->hdr.type = MSG_COORD_CTRL_REQ;
        req->type = HEARTBEAT;
        req->server_id = server_id;
        if (!send_msg(coord_fd_out, req, sizeof(*req))) {
            set_server_state(SHUT_DOWN, 2);
        }
        sleep(HEARTBEAT_INTERVAL);
    }

    pthread_exit(NULL);
}

// Initialize and start the server
static bool init_server()
{
    for (int i = 0; i < MAX_CLIENT_SESSIONS; i++)
    {
        client_fd_table[i] = -1;
    }

    // Get the host name that server is running on
    char my_host_name[HOST_NAME_MAX] = "";
    char timebuf[TIME_STR_SIZE];

    if (get_local_host_name(my_host_name, sizeof(my_host_name)) < 0)
    {
        return false;
    }
    log_write("%s Server starts on host: %s\n",
              current_time_str(timebuf, TIME_STR_SIZE), my_host_name);

    // Create sockets for incoming connections from clients and other servers
    uint16_t newport = 0;
    my_clients_fd = create_server(clients_port, MAX_CLIENT_SESSIONS, &newport);
    if (my_clients_fd < 0)
    {
        goto cleanup;
    }

    if (newport != 0)
    {
        clients_port = newport;
        newport = 0;
    }

    my_servers_fd = create_server(servers_port, MAX_SERVER_SESSIONS, &newport);
    if (my_servers_fd < 0)
    {
        goto cleanup;
    }
    if (newport != 0)
    {
        servers_port = newport;
        newport = 0;
    }

    my_coord_fd = create_server(coord_in_port, 1, &newport);
    if (my_coord_fd < 0)
    {
        goto cleanup;
    }
    if (newport != 0)
    {
        coord_in_port = newport;
        newport = 0;
    }

    // Determine the ids of replica servers
    primary_sid = primary_server_id(server_id, num_servers);
    secondary_sid = secondary_server_id(server_id, num_servers);

    // Initialize key-value storage
    if (!hash_init(&primary_hash, hash_size) || !hash_init(&secondary_hash, hash_size))
    {
        goto cleanup;
    }

    // Connect to coordinator to "register" that we are live
    if ((coord_fd_out = connect_to_server(coord_host_name, coord_port)) < 0)
    {
        goto cleanup;
    }
    // Tell coordinator about the port numbers we are using
    char send_buffer[MAX_MSG_LEN] = {0};
    coord_ctrl_request *req = (coord_ctrl_request *)send_buffer;
    req->hdr.type = MSG_COORD_CTRL_REQ;
    req->type = STARTED;
    req->server_id = server_id;
    req->ports[0] = clients_port;
    req->ports[1] = servers_port;
    req->ports[2] = coord_in_port;

    if (!send_msg(coord_fd_out, req, sizeof(*req) + 3 * sizeof(uint16_t)))
    {
        goto cleanup;
    }

    // Create a separate thread that takes care of sending periodic heartbeat messages
    if (pthread_create(&hearbeat_thread, NULL, run_heartbeat, NULL) != 0)
    {
        log_perror("pthread\n");
        goto cleanup;
    }

    log_write("Server initialized\n");
    return true;

cleanup:
    log_write("Server initialization failed.\n");
    cleanup();
    return false;
}

// Hash iterator for freeing memory used by values; called during storage cleanup
static void clean_iterator_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
    (void)key;
    (void)value_sz;
    (void)arg;

    assert(value != NULL);
    free(value);
}

// Cleanup and release all the resources
static void cleanup()
{
    log_write("Cleaning up and exiting ...\n");

    close_safe(&coord_fd_out);
    close_safe(&coord_fd_in);
    close_safe(&my_clients_fd);
    close_safe(&my_coord_fd);
    close_safe(&secondary_fd);

    for (int i = 0; i < MAX_CLIENT_SESSIONS; i++)
    {
        close_safe(&(client_fd_table[i]));
    }
    for (int i = 0; i < MAX_SERVER_SESSIONS; i++)
    {
        close_safe(&(server_fd_table[i]));
    }

    hash_iterate(&primary_hash, clean_iterator_f, NULL);
    hash_cleanup(&primary_hash);

    hash_iterate(&secondary_hash, clean_iterator_f, NULL);
    hash_cleanup(&secondary_hash);
    log_write("Waiting for threads to finish up their jobs...\n");
    if ((!pthread_equal(server_listener_thread, pthread_self()) && (pthread_join(server_listener_thread, NULL) != 0)) || 
        (!pthread_equal(hearbeat_thread, pthread_self()) && (pthread_join(hearbeat_thread, NULL) != 0)) ||
        (!pthread_equal(recovery_thread, pthread_self()) && (pthread_join(recovery_thread, NULL) != 0)))
    {
        log_perror("pthread join\n");
    }
}

// Connection will be closed after calling this function regardless of result
static void process_client_message(int fd)
{
    char timebuf[TIME_STR_SIZE];

    log_write("%s Receiving a client message\n",
              current_time_str(timebuf, TIME_STR_SIZE));

    // Read and parse the message
    char req_buffer[MAX_MSG_LEN] = {0};
    if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ))
    {
        return;
    }
    operation_request *request = (operation_request *)req_buffer;

    // Initialize the response
    char resp_buffer[MAX_MSG_LEN] = {0};
    operation_response *response = (operation_response *)resp_buffer;
    response->hdr.type = MSG_OPERATION_RESP;
    uint16_t value_sz = 0;


    // Check that requested key is valid.
    // A server should only respond to requests for which it holds the
    // primary replica. For debugging and testing, however, we also want
    // to allow the secondary server to respond to OP_VERIFY requests,
    // to confirm that replication has succeeded. To check this, we need
    // to know the primary server id for which this server is the secondary.
    int key_srv_id = key_server_id(request->key, num_servers);
    if ((key_srv_id != server_id) &&
        ((key_srv_id != primary_sid) || ((request->type != OP_VERIFY) && (get_server_state(0) != PRIMARY_RECOVERY))))
    {
        log_error("sid %d: Invalid client key %s sid %d\n", server_id, key_to_str(request->key), key_srv_id);
        // This can happen if client is using old config during recovery
        response->status = INVALID_REQUEST;
        send_msg(fd, response, sizeof(*response) + value_sz);
        return;
    }

    hash_table *target_hash = key_srv_id == primary_sid ? &secondary_hash : &primary_hash;

    // Process the request based on its type
    switch (request->type)
    {
    case OP_NOOP:
        response->status = SUCCESS;
        break;

    case OP_GET:
    case OP_VERIFY:
    {
        void *data = NULL;
        size_t size = 0;

        // secondary_fd == -1 -> still reconstructing 
        if (secondary_fd == -1) {
            response->status = SERVER_FAILURE;
            break;
        }

        // Get the value for requested key from the hash table

        hash_lock(target_hash, request->key);
        if (!hash_get(target_hash, request->key, &data, &size))
        {
            log_write("Key %s not found\n", key_to_str(request->key));
            hash_unlock(target_hash, request->key);
            response->status = KEY_NOT_FOUND;
            break;
        }

        // Copy the stored value into the response buffer
        memcpy(response->value, data, size);
        value_sz = size;
        hash_unlock(target_hash, request->key);

        response->status = SUCCESS;
        break;
    }

    case OP_PUT:
    {
        // Need to copy the value to dynamically allocated memory
        size_t value_size = request->hdr.length - sizeof(*request);
        int relay_fd = get_server_state(0) == PRIMARY_RECOVERY ? primary_fd : secondary_fd;
        void *value_copy = malloc(value_size);
        if (relay_fd == -1)
        {
            response->status = SERVER_FAILURE;
            break;
        }

        if (value_copy == NULL)
        {
            log_error("sid %d: Out of memory\n", server_id);
            response->status = OUT_OF_SPACE;
            break;
        }
        memcpy(value_copy, request->value, value_size);

        void *old_value = NULL;
        size_t old_value_sz = 0;

        hash_lock(target_hash, request->key);

        // Put the <key, value> pair into the hash table
        if (!hash_put(target_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
        {
            log_error("sid %d: Out of memory\n", server_id);
            free(value_copy);
            response->status = OUT_OF_SPACE;
            hash_unlock(target_hash, request->key);
            break;
        }

        // forward the PUT request to the secondary replica
        operation_response sec_response = {0};
        if (!send_msg(relay_fd, request, sizeof(*request) + strlen(request->value) + 1) ||
            !recv_msg(relay_fd, &sec_response, sizeof(sec_response), MSG_OPERATION_RESP) ||
            (sec_response.status != SUCCESS))
        {
            log_error("Fail to send to secondary\n");
            response->status = SERVER_FAILURE;
        }

        // Need to free the old value (if there was any)
        if (old_value != NULL)
        {
            // update failed, restore the old value
            if (response->status == SERVER_FAILURE)
            {
                hash_put(target_hash, request->key, old_value, old_value_sz, &old_value, &old_value_sz);
            }
            free(old_value);
        }
        hash_unlock(target_hash, request->key);

        response->status = SUCCESS;
        break;
    }

    default:
        log_error("sid %d: Invalid client operation type\n", server_id);
        return;
    }

    // Send reply to the client
    if (!send_msg(fd, response, sizeof(*response) + value_sz)) {
        log_error("client reply failed\n");
    }
}

// Returns false if either the message was invalid or if this was the last message
// (in both cases the connection will be closed)
static bool process_server_message(int fd)
{
    char timebuf[TIME_STR_SIZE];

    log_write("%s Receiving a server message\n",
              current_time_str(timebuf, TIME_STR_SIZE));

    // Read and parse the message
    char req_buffer[MAX_MSG_LEN] = {0};
    if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ))
    {   
        return false;
    }
    operation_request *request = (operation_request *)req_buffer;

    // NOOP operation request is used to indicate the last message in an UPDATE sequence
    if (request->type == OP_NOOP)
    {
        log_write("Received the last server message, closing connection\n");
        return false;
    }

    // process the message and send the response

    operation_response response = {0};
    response.hdr.type = MSG_OPERATION_RESP;
    int key_srv_id = key_server_id(request->key, num_servers);
    if (request->type != OP_PUT || (key_srv_id != primary_sid && key_srv_id != server_id))
    {   
        response.status = INVALID_REQUEST;
        goto send_;
    }

    hash_table *target_hash = key_srv_id == primary_sid ? &secondary_hash : &primary_hash;

    size_t value_size = request->hdr.length - sizeof(*request);
    void *value_copy = malloc(value_size);

    if (value_copy == NULL)
    {
        log_perror("malloc\n");
        log_error("sid %d: Out of memory\n", server_id);
        response.status = OUT_OF_SPACE;
        goto send_;
    }
    memcpy(value_copy, request->value, value_size);

    void *old_value = NULL;
    size_t old_value_sz = 0;

    hash_lock(target_hash, request->key);

    // Put the <key, value> pair into the hash table
    if (!hash_put(target_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
    {
        log_error("sid %d: Out of memory\n", server_id);
        free(value_copy);
        response.status = OUT_OF_SPACE;
    }

    hash_unlock(target_hash, request->key);
    response.status = SUCCESS;

send_:
    send_msg(fd, &response, sizeof(response));
    if (response.status == INVALID_REQUEST)
    {
        return false;
    }
    return true;
}

static void dump_hash_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{   
    char *value_str = (char*)value;
    value_str[value_sz - 1] = 0;
    FILE *fp = (FILE *)arg;
    fprintf(fp, "hash: %s, value: %s\n", key_to_str(key), value_str);
}

static bool recovery_fail = false;
static void sent_entry_hash_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
    int *server_fd = (int *)arg;
    char send_buffer[MAX_MSG_LEN] = {0};
    operation_request *request = (operation_request *)send_buffer;
    request->hdr.type = MSG_OPERATION_REQ;
    request->type = OP_PUT;
    memcpy(request->key, key, KEY_SIZE);
    strncpy(request->value, (char *)value, value_sz);
    operation_response response = {0};
    if (!send_msg(*server_fd, request, sizeof(*request) + strlen(request->value) + 1) ||
        !recv_msg(*server_fd, &response, sizeof(response), MSG_OPERATION_RESP) ||
        (response.status != SUCCESS))
    {
        log_error("sid %d: fail to send recovery data\n", server_id);
        recovery_fail = true;
    }
}

static void *run_recovery_thread(void *args)
{
    (void)args;
    recovery_fail = false;
    update_server_state(3);
	if ((get_server_state(3) != PRIMARY_RECOVERY) && (get_server_state(3) != SECONDARY_RECOVERY)) {
		pthread_exit(NULL);
	}
    int server_fd = get_server_state(3) == PRIMARY_RECOVERY ? primary_fd : secondary_fd;
    hash_table *target_hash = get_server_state(3) == PRIMARY_RECOVERY ? &secondary_hash : &primary_hash;
    hash_iterate(target_hash, sent_entry_hash_f, &server_fd);
    if (get_server_state(3) == PRIMARY_RECOVERY)
    {
        operation_request op = {0};
        op.hdr.type = MSG_OPERATION_REQ;
        op.type = OP_NOOP;
        if (!send_msg(server_fd, &op, sizeof(op)))
        {
            log_error("Unable to send NOOP request\n");
            recovery_fail = true;
        }
    }

    coord_ctrl_request request = {0};
    request.hdr.type = MSG_COORD_CTRL_REQ;
    request.server_id = server_id;
    if (recovery_fail)
    {
        request.type = get_server_state(3) == PRIMARY_RECOVERY ? UPDATE_PRIMARY_FAILED : UPDATE_SECONDARY_FAILED;
    }
    else
    {
        request.type = get_server_state(3) == PRIMARY_RECOVERY ? UPDATED_PRIMARY : UPDATED_SECONDARY;
    }
    if (!send_msg(coord_fd_out, &request, sizeof(request)))
    {
        log_error("send update completed msg failed");
    }
    else if (get_server_state(3) == SECONDARY_RECOVERY)
    {   
        set_server_state(NORMAL, 3);
    }
    pthread_exit(NULL);
}

// Returns false if the message was invalid (so the connection will be closed)
// Sets *shutdown_requested to true if received a SHUTDOWN message (so the server will terminate)
static bool process_coordinator_message(int fd, bool *shutdown_requested)
{
    char timebuf[TIME_STR_SIZE];

    assert(shutdown_requested != NULL);
    *shutdown_requested = false;

    log_write("%s Receiving a coordinator message\n",
              current_time_str(timebuf, TIME_STR_SIZE));

    // Read and parse the message
    char req_buffer[MAX_MSG_LEN] = {0};
    if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_SERVER_CTRL_REQ))
    {
        return false;
    }
    server_ctrl_request *request = (server_ctrl_request *)req_buffer;

    // Initialize the response
    server_ctrl_response response = {0};
    response.hdr.type = MSG_SERVER_CTRL_RESP;
    response.status = SERVER_CTRLREQ_STATUS_MAX; // Detect unset status

    // Process the request based on its type

    FILE *fp;
    char server_name[PATH_MAX];
    switch (request->type)
    {
    case SET_SECONDARY:
        if ((secondary_fd = connect_to_server(request->host_name,
                                              request->port)) < 0)
        {
            log_error("sid %d Set Secondary failed\n", server_id);
            response.status = CTRLREQ_FAILURE;
        }
        else
        {
            response.status = CTRLREQ_SUCCESS;
        }
        break;

    case UPDATE_PRIMARY:
        if ((get_server_state(0) != NORMAL) ||
            ((primary_fd = connect_to_server(request->host_name, request->port)) < 0))
        {
            response.status = CTRLREQ_FAILURE;
        }
        else
        {
            set_server_state(PRIMARY_RECOVERY, 0);
            
            response.status = CTRLREQ_SUCCESS;
        }
        break;

    case UPDATE_SECONDARY:
        if ((get_server_state(0) != NORMAL) ||
            ((secondary_fd = connect_to_server(request->host_name, request->port)) < 0))
        {
            response.status = CTRLREQ_FAILURE;
        }
        else
        {
            set_server_state(SECONDARY_RECOVERY, 0);
            response.status = CTRLREQ_SUCCESS;
        }
        break;

    case SWITCH_PRIMARY:
        if (get_server_state(0) != PRIMARY_RECOVERY)
        {
            response.status = CTRLREQ_FAILURE;
        }
        else
        {
            response.status = CTRLREQ_SUCCESS;
            set_server_state(NORMAL, 0);
        }
        break;

    case SHUTDOWN:
        *shutdown_requested = true;
        return true;

    case DUMP_PRIMARY:
        // DONE: write primary keys from hash table to file, overwriting
        // any previous content in the output file.
        // The output file should be named "server_<sid>.primary",
        // where <sid> is the server id number of this server.
        // No response is expected, so after dumping keys, just return.
        memset(server_name, 0, PATH_MAX);
        sprintf(server_name, "server_%d.primary", server_id);
        fp = fopen(server_name, "w");
        hash_iterate(&primary_hash, dump_hash_f, fp);
        fclose(fp);
        return true;

    case DUMP_SECONDARY:
        // DONE: write secondary keys from hash table to file, overwriting
        // any previous content in the output file.
        // The output file should be named "server_<sid>.secondary",
        // where <sid> is the server id number of this server.
        // No response is expected, so after dumping keys, just return.
        memset(server_name, 0, PATH_MAX);
        sprintf(server_name, "server_%d.secondary", server_id);
        fp = fopen(server_name, "w");
        hash_iterate(&secondary_hash, dump_hash_f, fp);
        fclose(fp);
        return true;

    default: // impossible
        assert(false);
        break;
    }

    assert(response.status != SERVER_CTRLREQ_STATUS_MAX);

    if (!send_msg(fd, &response, sizeof(response))) {
        log_error("send coord response failed\n");
        return false;
    }
    if (((request->type == UPDATE_PRIMARY) || (request->type == UPDATE_SECONDARY))) {
        pthread_create(&recovery_thread, NULL, run_recovery_thread, NULL);
    }
    return true;
}

static void *run_server_listener_loop(void *args)
{
    (void)args;
    fd_set rset, allset;
    FD_ZERO(&allset);
    FD_SET(my_servers_fd, &allset);
    int maxfd = my_servers_fd;

    for (;;)
    {
        rset = allset;

        if (select(maxfd + 1, &rset, NULL, NULL, NULL) < 0) {
            log_perror("select\n");
            set_server_state(SHUT_DOWN, 1);
        }

        if (update_server_state(1) == SHUT_DOWN) {
            pthread_exit(NULL);
        }

        // Incoming connection from server
        if (FD_ISSET(my_servers_fd, &rset))
        {   
            int fd_idx = accept_connection(my_servers_fd, server_fd_table, MAX_SERVER_SESSIONS);
            if (fd_idx >= 0)
            {   
                FD_SET(server_fd_table[fd_idx], &allset);
                maxfd = max(maxfd, server_fd_table[fd_idx]);
            }
        }

        // Check for any messages from the primary server
        for (int i = 0; i < MAX_SERVER_SESSIONS; i++)
        {
            if ((server_fd_table[i] != -1) && FD_ISSET(server_fd_table[i], &rset))
            {   
                if (!process_server_message(server_fd_table[i]))
                {
                    // Received an invalid message, close the connection
                    log_error("sid %d: Closing server connection\n", server_id);
                    FD_CLR(server_fd_table[i], &allset);
                    close_safe(&(server_fd_table[i]));
                }
            }
        }
    }
    close_safe(&my_servers_fd);
    close_safe(&primary_fd);
    pthread_exit(NULL);
}

// Returns false if stopped due to errors, true if shutdown was requested
static bool run_server_loop()
{
    // Usual preparation stuff for select()
    fd_set rset, allset;
    FD_ZERO(&allset);
    FD_SET(my_clients_fd, &allset);
    FD_SET(my_coord_fd, &allset);

    int maxfd = max(my_servers_fd, max(my_clients_fd, my_coord_fd));

    // Server sits in an infinite loop waiting for incoming connections
    // from coordinator or clients, and for incoming messages from already
    // connected coordinator or clients
    
    for (;;)
    {
        rset = allset;
        int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);
        if ((update_server_state(0) == SHUT_DOWN) || (num_ready_fds < 0)) {
            set_server_state(SHUT_DOWN, 0);
            return false;
        }

        if (num_ready_fds <= 0)
        {
            continue;
        }

        // Incoming connection from the coordinator
        if (FD_ISSET(my_coord_fd, &rset))
        {
            int fd_idx = accept_connection(my_coord_fd, &coord_fd_in, 1);
            if (fd_idx >= 0)
            {
                FD_SET(coord_fd_in, &allset);
                maxfd = max(maxfd, coord_fd_in);
            }
            assert(fd_idx == 0);

            if (--num_ready_fds <= 0)
            {
                continue;
            }
        }

        // Check for any messages from the coordinator
        if ((coord_fd_in != -1) && FD_ISSET(coord_fd_in, &rset))
        {
            bool shutdown_requested = false;
            if (!process_coordinator_message(coord_fd_in,
                                             &shutdown_requested))
            {
                // Received an invalid message, close the connection
                log_error("sid %d: Closing coordinator connection\n", server_id);
                FD_CLR(coord_fd_in, &allset);
                close_safe(&(coord_fd_in));
            }
            else if (shutdown_requested)
            {   
                set_server_state(SHUT_DOWN, 0);
                return true;
            }

            if (--num_ready_fds <= 0)
            {
                continue;
            }
        }

        // Incoming connection from a client
        if (FD_ISSET(my_clients_fd, &rset))
        {
            int fd_idx = accept_connection(my_clients_fd, client_fd_table, MAX_CLIENT_SESSIONS);
            if (fd_idx >= 0)
            {
                FD_SET(client_fd_table[fd_idx], &allset);
                maxfd = max(maxfd, client_fd_table[fd_idx]);
            }

            if (--num_ready_fds <= 0)
            {
                continue;
            }
        }

        // Check for any messages from connected clients
        for (int i = 0; i < MAX_CLIENT_SESSIONS; i++)
        {
            if ((client_fd_table[i] != -1) && FD_ISSET(client_fd_table[i], &rset))
            {
                process_client_message(client_fd_table[i]);
                // Close connection after processing (semantics are "one connection per request")
                FD_CLR(client_fd_table[i], &allset);
                close_safe(&(client_fd_table[i]));

                if (--num_ready_fds <= 0)
                {
                    break;
                }
            }
        }
    }
    return true;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    if (!parse_args(argc, argv))
    {
        usage(argv);
        return 1;
    }

    open_log(log_file_name);

    hearbeat_thread = pthread_self();
    server_listener_thread = pthread_self();
    recovery_thread = pthread_self();

    if (!init_server())
    {
        return 1;
    }

    if (pthread_create(&server_listener_thread, NULL, run_server_listener_loop, NULL) != 0)
    {
        log_perror("pthread\n");
        return 1;
    }
    bool result = run_server_loop();

    cleanup();
    return result ? 0 : 1;
}


