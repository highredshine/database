#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "./comm.h"
#include "./db.h"
#ifdef __APPLE__
#include "pthread_OSX.h"
#endif

/*
 * Use the variables in this struct to synchronize your main thread with client
 * threads. Note that all client threads must have terminated before you clean
 * up the database.
 */
typedef struct server_control {
    pthread_mutex_t server_mutex;
    pthread_cond_t server_cond;
    int num_client_threads;
} server_control_t;

/*
 * Controls when the clients in the client thread list should be stopped and
 * let go.
 */
typedef struct client_control {
    pthread_mutex_t go_mutex;
    pthread_cond_t go;
    int stopped;
} client_control_t;

/*
 * The encapsulation of a client thread, i.e., the thread that handles
 * commands from clients.
 */
typedef struct client {
    pthread_t thread;
    FILE *cxstr;  // File stream for input and output

    // For client list
    struct client *prev;
    struct client *next;
} client_t;

/*
 * The encapsulation of a thread that handles signals sent to the server.
 * When SIGINT is sent to the server all client threads should be destroyed.
 */
typedef struct sig_handler {
    sigset_t set;
    pthread_t thread;
} sig_handler_t;

client_t *thread_list_head;
pthread_mutex_t thread_list_mutex = PTHREAD_MUTEX_INITIALIZER;

// client and server thread controllers
client_control_t client_control = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 1};
server_control_t server_control = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0};

void *run_client(void *arg);
void *monitor_signal(void *arg);
void thread_cleanup(void *arg);

// in client_control_wait function, pthread_mutex_unlock function is used
// as a cleanup routine. However, pthread_cleanup_push function takes
// void* as the argument for the cleanup routine function, but
// pthread_mutex_unlock needs pthread_mutex_t* as its argument.
// This modified routine function thus solves this issue by wrapping it.
void cleanup_mutex_unlock(void *mutex) {
    pthread_mutex_unlock((pthread_mutex_t *) mutex);
}

// Called by client threads to wait until progress is permitted
void client_control_wait() {
    // TODO: Block the calling thread until the main thread calls
    // client_control_release(). See the client_control_t struct.
    // lock client control mutex object before modifying the client control
    int e = pthread_mutex_lock(&client_control.go_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on client_control for client_control_wait failed. \n");
    } 
    // push unlock function for cleanup
    pthread_cleanup_push(&cleanup_mutex_unlock, (void *) &client_control.go_mutex);
    // block the marked client and wait.
    while(!client_control.stopped){
        e = pthread_cond_wait(&client_control.go, &client_control.go_mutex);
        if (e){
            handle_error_en(e, "pthread_cond_wait for client_control_wait failed. \n");
        }
    }
    // pop the cleanup when wait is over.
    pthread_cleanup_pop(1);
}

// Called by main thread to stop client threads
void client_control_stop() {
    // TODO: Ensure that the next time client threads call client_control_wait()
    // at the top of the event loop in run_client, they will block.
    // lock client control mutex object before modifying the client control
    int e = pthread_mutex_lock(&client_control.go_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on client_control for client_control_stop failed. \n");
    }    
    // change the stopped boolean to 0 to block the clients.
    client_control.stopped = 0;
    // unlock client control mutex object after modifying the client control
    e = pthread_mutex_unlock(&client_control.go_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_unlock on client_control for client_control_stop failed. \n");
    }
}

// Called by main thread to resume client threads
void client_control_release() {
    // TODO: Allow clients that are blocked within client_control_wait()
    // to continue. See the client_control_t struct.
    // lock client control mutex object before modifying the client control
    int e = pthread_mutex_lock(&client_control.go_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on client_control for client_control_release failed. \n");
    }    
    // change the stopped boolean to 1.
    client_control.stopped = 1;
    // broadcast the signal to unblock the clients.
    e = pthread_cond_broadcast(&client_control.go);
    if (e){
        handle_error_en(e, "pthread_cond_broadcast for client_control_release failed. \n");
    }
    // unlock client control mutex object after modifying the client control
    e = pthread_mutex_unlock(&client_control.go_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_unlock on client_control for client_control_release failed. \n");
    }
}

// Called by listener (in comm.c) to create a new client thread
void client_constructor(FILE *cxstr) {
    // You should create a new client_t struct here and initialize ALL
    // of its fields. Remember that these initializations should be
    // error-checked.
    //
    // TODO:
    // Step 1: Allocate memory for a new client and set its connection stream
    // to the input argument.
    client_t *client = malloc(sizeof(client_t));
    if (client == NULL) {
        perror("malloc for client_constructor failed. \n");
        exit(1);
    }
    client->cxstr = cxstr;
    // initialize the prev and next pointers.
    client->prev = NULL;
    client->next = NULL;
    // Step 2: Create the new client thread running the run_client routine.
    int e = pthread_create(&client->thread, 0, run_client, client);
    if (e) {
        handle_error_en(e, "pthread_create for client_constructor failed. \n");
    }
    e = pthread_detach(client->thread);
    if (e) {
        handle_error_en(e, "pthread_detach for client_constructor failed. \n");
    }
}

void client_destructor(client_t *client) {
    // TODO: Free all resources associated with a client.
    // Whatever was malloc'd in client_constructor should
    // be freed here!
    comm_shutdown(client->cxstr);
    free(client);
}

// boolean to represent if the server is still accpeting clients. 
int client_is_accepting = 1;

// Code executed by a client thread
void *run_client(void *arg) {
    client_t *client = arg;
    // TODO:
    // Step 1: Make sure that the server is still accepting clients.
    if (!client_is_accepting) {
        client_destructor(client);
        return NULL;
    }
    // Step 2: Add client to the client list and push thread_cleanup to remove
    //       it if the thread is canceled.
    // lock mutex object before operating on the client.
    int e = pthread_mutex_lock(&thread_list_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on thread list for run_client failed. \n");
    }
    // case 1: if the thread list is empty
    if (thread_list_head == NULL) {
        thread_list_head = client;
    } else {
        // case 2: if not empty, reach to the end of the list.
        client_t *cur = thread_list_head;
        while (cur->next) {
            cur = cur->next;
        }
        // add client to the list
        client->prev = cur;
        client->next = NULL;
        cur->next = client;
    }
    // push the thread_cleanup function to cleanup handler.
    pthread_cleanup_push(&thread_cleanup, client);
    // lock server control mutex object before modifying the number of clients
    e = pthread_mutex_lock(&server_control.server_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on server_control for run_client failed. \n");
    }    
    // increase the number of clients on ther server.
    server_control.num_client_threads++;
    // unlock server control mutex object after modifying the number of clients
    e = pthread_mutex_unlock(&server_control.server_mutex);
    if (e) {
        handle_error_en(e, "pthread_mutex_unlock on server_control for run_client failed. \n");
    }
    // unlock mutex object after operating on the client.
    e = pthread_mutex_unlock(&thread_list_mutex);
    if (e) {
        handle_error_en(e, "pthread_mutex_unlock on thread list for run_client failed. \n");
    }   
    // Step 3: Loop comm_serve (in comm.c) to receive commands and output
    //       responses. Note that the client may terminate the connection at
    //       any moment, in which case reading/writing to the connection stream
    //       on the server side will send this process a SIGPIPE. You must
    //       ensure that the server doesn't crash when this happens!
    // signal handling
    sigset_t set;
    // intialize signal set to empty
    if (sigemptyset(&set) < 0){
        perror("sigemptyset for run_client failed. \n");
        exit(1);
    }
    // add signal from SIGPIPE
    if (sigaddset(&set, SIGPIPE) < 0){
        perror("sigaddset for run_client failed. \n");
        exit(1);
    }
    // examine and change mask of blocked signals
    e = pthread_sigmask(SIG_BLOCK, &set, 0);
    if (e){
        handle_error_en(e, "pthread_sigmask for run_client failed. \n");
    }
    // receive commands and output responses:
    char response[BUFLEN], command[BUFLEN];
    memset(response, 0, BUFLEN);
    memset(command, 0, BUFLEN);
    while(comm_serve(client->cxstr, response, command) == 0) {
        client_control_wait();
        interpret_command(command, response, BUFLEN);
    }
    // Step 4: When the client is done sending commands, exit the thread
    //       cleanly.
    // set the cancelability state of the thread to disabled.
    e = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    if (e) {
        handle_error_en(e, "pthread_setcancelstate for run_client failed. \n");
    }
    // pop the cleanup handler when client operation is over.
    pthread_cleanup_pop(1);
    // Keep the signal handler thread in mind when writing this function!
    return NULL;
}

void delete_all() {
    // TODO: Cancel every thread in the client thread list with the
    // pthread_cancel function.
    // lock mutex object before deleting the threads.
    int e = pthread_mutex_lock(&thread_list_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on thread list for delete_all failed. \n");
    }
    // loop to delete each thread in the thread list.
    client_t* cur = thread_list_head;
    while (cur){
        e = pthread_cancel(cur->thread);
        if (e){
            handle_error_en(e, "pthread_cancel for delete_all failed. \n");
        }
        cur = cur->next;
    }
    // mark the server as no clients are further accepted.
    client_is_accepting = 0;
    // unlock mutex object after deleting the threads.
    e = pthread_mutex_unlock(&thread_list_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_unlock on thread list for delete_all failed. \n");
    }   
}

// Cleanup routine for client threads, called on cancels and exit.
void thread_cleanup(void *arg) {
    // TODO: Remove the client object from thread list and call
    // client_destructor. This function must be thread safe! The client must
    // be in the list before this routine is ever run.
    // lock mutex object before cleaning up the threads.
    int e = pthread_mutex_lock(&thread_list_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on thread list for thread_cleanup failed. \n");
    }
    // reassign arg as client_t object.
    client_t *client = arg;
    // edge case: if the thread list has only one left:
    if (thread_list_head == client) {
        thread_list_head = NULL;
    }
    // update the prev and next pointers.
    if (client->next) client->next->prev = client->prev;
    if (client->prev) client->prev->next = client->next;
    // shut down the client and free its resources
    client_destructor(client);
    // lock server control mutex object before modifying the number of clients
    e = pthread_mutex_lock(&server_control.server_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on server_control for thread_cleanup failed. \n");
    }    
    // decrease the number of clients on ther server.
    server_control.num_client_threads--;
    // send the signal to the main thread when the list is empty.
    if (server_control.num_client_threads == 0) {
        e = pthread_cond_signal(&server_control.server_cond);
        if (e){
            handle_error_en(e, "pthread_cond_signal for thread_cleanup failed. \n");
        }
    }
    // unlock server control mutex object after modifying the number of clients
    e = pthread_mutex_unlock(&server_control.server_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_unlock on server_control for thread_cleanup failed. \n");
    }
    // lock mutex object after cleaning up the threads.
    e = pthread_mutex_unlock(&thread_list_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_unlock on thread list for thread_cleanup failed. \n");
    }   
}

// Code executed by the signal handler thread. For the purpose of this
// assignment, there are two reasonable ways to implement this.
// The one you choose will depend on logic in sig_handler_constructor.
// 'man 7 signal' and 'man sigwait' are both helpful for making this
// decision. One way or another, all of the server's client threads
// should terminate on SIGINT. The server (this includes the listener
// thread) should not, however, terminate on SIGINT!
void *monitor_signal(void *arg) {
    // TODO: Wait for a SIGINT to be sent to the server process and cancel
    // all client threads when one arrives.
    int sig;
    while (1) {
        // sigwait returns positive error number
        if (sigwait((sigset_t*) arg, &sig)) {
            perror("sigwait for monitor_signal failed. \n");
            exit(1);
        // sigwait returns 0 if sucessful.
        } else {
            // cancel clients.
            delete_all();
            printf("all client threads are cancelled. \n");
            // mark client_is_accepting as true.
            client_is_accepting = 1;
        }
    }
    return NULL;
}

sig_handler_t *sig_handler_constructor() {
    // TODO: Create a thread to handle SIGINT. The thread that this function
    // creates should be the ONLY thread that ever responds to SIGINT.
    // initialize
    sig_handler_t * sig_handler = malloc(sizeof(sig_handler_t));
    if (sig_handler == NULL) {
        perror("malloc for sig_handler_constructor failed. \n");
        exit(1);
    }
    // intialize signal set to empty
    if (sigemptyset(&sig_handler->set) < 0){
        perror("sigemptyset for sig_handler_constructor failed. \n");
        exit(1);
    }
    // add signal from SIGINT
    if (sigaddset(&sig_handler->set, SIGINT) < 0){
        perror("sigaddset for sig_handler_constructor failed. \n");
        exit(1);
    }
    // examine and change mask of blocked signals
    int e = pthread_sigmask(SIG_BLOCK, &sig_handler->set, 0);
    if (e){
        handle_error_en(e, "pthread_sigmask for sig_handler_constructor failed. \n");
    }
    // create thread
    e = pthread_create(&sig_handler->thread, 0, monitor_signal, &sig_handler->set);
    if (e) {
        handle_error_en(e, "pthread_create for sig_handler_constructor failed. \n");
    }
    // detach thread
    e = pthread_detach(sig_handler->thread);
    if (e) {
        handle_error_en(e, "pthread_detach for sig_handler_constructor failed. \n");
    }
    return sig_handler;
}

void sig_handler_destructor(sig_handler_t *sig_handler) {
    // TODO: Free any resources allocated in sig_handler_constructor.
    int e = pthread_cancel(sig_handler->thread);
    if (e) {
        handle_error_en(e, "pthread_cancel for sig_handler_destructor failed. \n");
    }
    free(sig_handler);
}

// The arguments to the server should be the port number.
int main(int argc, char *argv[]) {
    // TODO:
    // preliminary check: ./server should be called with port number.
    if (argc != 2) {
        perror("Usage: server <port> \n");
        exit(1);
    }
    // Step 1: Set up the signal handler.
    sig_handler_t *sig_handler = sig_handler_constructor();
    // Step 2: Start a listener thread for clients (see start_listener in
    //       comm.c).
    pthread_t listener = start_listener(atoi(argv[1]), &client_constructor);
    // Step 3: Loop for command line input and handle accordingly until EOF.
    // buffer to store command line input
    char buffer[BUFLEN];
    memset(buffer, 0, BUFLEN);
    ssize_t cmd_input_size = read(STDIN_FILENO, buffer, BUFLEN);
    if (cmd_input_size < 0){
        perror("reading comnad line input failed: \n");
        exit(1);
    } 
    // loop until EOF (when command line input size is 0).
    while (cmd_input_size > 0) {
        // stop command:
        if (buffer[0] == 's') {
            client_control_stop();
        // go command:
        } else if (buffer[0] == 'g') {
            client_control_release();
        // print command:
        } else if (buffer[0] == 'p') {
            db_print(strtok(&buffer[1], "\t\n "));
        }
        // next command line input
        memset(buffer, 0, BUFLEN);
        cmd_input_size = read(STDIN_FILENO, buffer, BUFLEN);
        if (cmd_input_size < 0){
            perror("reading comnad line input failed: \n");
            exit(1);
        } 
    }
    // Step 4: Destroy the signal handler(1), delete all clients(2), cleanup the
    //       database(3), cancel the listener thread(4), and exit (5).
    // before processing the exit phase, print exit clause on the REPL.
    if (fprintf(stdout, "exiting database\n") < 0){
        perror("fprintf failure: \n");
        exit(1);
    }
    // (1)
    sig_handler_destructor(sig_handler);
    // (2)
    delete_all();
    // lock server control mutex object before waiting for each client.
    int e = pthread_mutex_lock(&server_control.server_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_lock on server_control for main failed. \n");
    }
    // call wait function for each client until num_client_threads reaches 0.
    while (server_control.num_client_threads > 0) {
        e = pthread_cond_wait(&server_control.server_cond, &server_control.server_mutex);
        if (e){
            handle_error_en(e, "pthread_cond_wait for main failed. \n");
        }
    }
    // unlock client control mutex object after making sure all clients are deleted.
    e = pthread_mutex_unlock(&server_control.server_mutex);
    if (e){
        handle_error_en(e, "pthread_mutex_unlock on server_control for main failed. \n");
    }
    // (3)
    db_cleanup();
    // (4)
    e = pthread_cancel(listener);
    if (e) {
        handle_error_en(e, "pthread_cancel for main failed. \n");
    }
    // destroy mutex objects.
    e = pthread_mutex_destroy(&thread_list_mutex);
    if (e) {
        handle_error_en(e, "pthread_mutex_destroy on thread_list for main failed. \n");
    }
    e = pthread_mutex_destroy(&server_control.server_mutex);
    if (e) {
        handle_error_en(e, "pthread_mutex_destroy on server_control for main failed. \n");
    }
    e = pthread_mutex_destroy(&client_control.go_mutex);
    if (e) {
        handle_error_en(e, "pthread_mutex_destroy on client_control for main failed. \n");
    }
    // destroy cond objects.
    e = pthread_cond_destroy(&server_control.server_cond);
    if (e) {
        handle_error_en(e, "pthread_cond_destroy on server_control for main failed. \n");
    }
    e = pthread_cond_destroy(&client_control.go);
    if (e) {
        handle_error_en(e, "pthread_cond_destroy on client_control for main failed. \n");
    }
    // (5)
    pthread_exit(0);
    // You should ensure that the thread list is empty before cleaning up the
    // database and canceling the listener thread. Think carefully about what
    // happens in a call to delete_all() and ensure that there is no way for a
    // thread to add itself to the thread list after the server's final
    // delete_all().
}
