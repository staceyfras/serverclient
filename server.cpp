/** This program illustrates the server end of the message queue **/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <signal.h>
#include <vector>
#include <list>
#include <string>
#include <fstream>

#include "msg.h"
using namespace std;

struct record
{
	/* The record id */
	int id;
	
	/* The first name */	
	string firstName;
		
	/* The first name */
	string lastName;
	
	/**
	 * Prints the record
	 * @param fp - the stream to print to
	 */
	void print(FILE* fp)
	{
		fprintf(stderr, "%d--%s--%s", id, firstName.c_str(), lastName.c_str());
	}
};



/**
 * The structure of a hashtable cell
 */
class hashTableCell
{
	/* Public members */
	public:	

	/**
 	 * Initialize the mutex
 	 */
	hashTableCell()
	{
		/* TODO: Initialize the mutex associated with this cell */
		hutex = PTHREAD_MUTEX_INITIALIZER;
	}
	
	/**
 	 * Initialize the mutex
 	 */
	~hashTableCell()
	{
		/* TODO: deallocate the mutex associated with the cell*/
		if(pthread_mutex_destroy(&hutex) !=  0) {
			perror("pthread_mutex_destroy");
			exit(-1);
		}
	}
	
	/**
 	 * Locks the cell mutex
 	 */
	void lockCell()
	{
		/* TODO: lock the mutex associated with this cell */
		if(pthread_mutex_lock(&hutex) != 0) {
			perror("pthread_mutex_lock");
			exit(-1);
		}
	}
	
	/**
 	 * Unlocks the cell mutex
 	 */
	void unlockCell()
	{
		/* TODO: unlock the mutex associated with this cell */
		if(pthread_mutex_unlock(&hutex) != 0) {
			perror("pthread_mutex_unlock");
			exit(-1);
		}
	}
		
	
	/* The linked list of records */
	list<record> recordList;
	
	
	/**
 	 * TODO: Declare the cell mutex
 	 */
	pthread_mutex_t hutex;
	
};

/**
* VARIABLE DECLARATIONS
*/
/* The number of cells in the hash table */
#define NUMBER_OF_HASH_CELLS 100

/* The number of inserter threads */
#define NUM_INSERTERS 5

/* The hash table */
vector<hashTableCell> hashTable(NUMBER_OF_HASH_CELLS);

/* The number of fetcher threads */
vector<pthread_t> fetcherThreadIds;

/* The thread ids */
vector<pthread_t> threadsIds(NUM_INSERTERS);

/* The number of threads */
int numThreads = 0;

/* The message queue id */
int msqid;

/* A flag indicating that it's time to exit */
bool timeToExit = false;

/* The number of threads that have exited */
int numExitedThreads = 0;
	
/* The ids that yet to be looked up: FIFO method used */
list<int> idsToLookUpList;

/* The mutex protecting idsToLookUpList */
pthread_mutex_t idsToLookUpListMutex = PTHREAD_MUTEX_INITIALIZER;


/**
 * Prototype for createInserterThreads
 */
void createInserterThreads();


/**
 * A prototype for adding new records.
 */
void* addNewRecords(void* arg);

/**
 * Deallocated the message queue
 * @param sig - the signal
 */
void cleanUp(int sig)
{

	/* Finally, deallocate the queue */
	if(msgctl(msqid, IPC_RMID, NULL) < 0)
	{
		perror("msgctl");
	}
	
	/* It's time to exit */
	timeToExit = true;
}

/**
 * Sends the message over the message queue
 * @param msqid - the message queue id
 * @param rec - the record to send
 */
void sendRecord(const int& msqid, const record& rec)
{
	/**
 	 * Convert the record to message
 	 */
		
	/* The message to send */
	message msg; 
	
	/* Copy fields from the record into the message queue */	
	msg.messageType = SERVER_TO_CLIENT_MSG;
	msg.id = rec.id;
	strncpy(msg.firstName, rec.firstName.c_str(), MAX_NAME_LEN);	
	strncpy(msg.lastName, rec.lastName.c_str(), MAX_NAME_LEN);
	
	/* Send the message */
	sendMessage(msqid, msg);		
}

/**
 * Prints the hash table
 */
void printHashTable()
{
	/* Go through the hash table */
	for(vector<hashTableCell>::const_iterator hashIt = hashTable.begin();
		hashIt != hashTable.end(); ++hashIt)
	{
		/* Go through the list at each hash location */
		for(list<record>::const_iterator lIt = hashIt->recordList.begin();
			lIt != hashIt->recordList.end(); ++lIt)
		{
			fprintf(stderr, "%d-%s-%s-%d\n", lIt->id, 
						lIt->firstName.c_str(), 
						lIt->lastName.c_str(),
						lIt->id % NUMBER_OF_HASH_CELLS
						);
		}
	}
}

/**
 * Adds a record to hashtable
 * @param rec - the record to add
 */
void addToHashTable(const record& rec)
{
	/**
 	 * TODO: Lock the mutex associated with the cell
 	 */
	int hash_key = rec.id % NUMBER_OF_HASH_CELLS;
	hashTable.at(hash_key).lockCell();
	
	/* TODO: Add the record to the cell */
	hashTable.at(hash_key).recordList.push_front(rec);
	
	/**
 	 * TODO: release mutex associated with the cell
 	 */
	hashTable.at(hash_key).unlockCell();
}


/**
 * Adds a record to hashtable
 * @param id the id of the record to retrieve
 * @return - the record from hashtable if exists;
 * otherwise returns a record with id field set to -1
 */
record getHashTableRecord(const int& id)
{
	/* The data structure to hold the record */
	record rec;
	rec.id = -1;
	
	/* TODO: Find the cell containing the record with the specificied ID. Search
	 * the linklist of records in that cell and see if its ID matches. If so, return that
	 * record. If not, return a record with ID of -1 indicating that no such record is present.
 	 * PLEASE REMEMBER: YOU NEED TO AVOID RACE CONDITIONS ON THE RECORDS IN THE SAME CELL.
	 * You will need to lock the appropriate cell mutex.
	 */
	int hash_key = id % NUMBER_OF_HASH_CELLS;
	hashTable.at(hash_key).lockCell();
	for(list<record>::const_iterator record = hashTable.at(hash_key).recordList.begin();
		record != hashTable.at(hash_key).recordList.end(); ++record)
	{
		if(record->id == id)
		{		
			rec.id = record->id;
			rec.firstName = record->firstName;
			rec.lastName = record->lastName;
			break;
		}
	}
	hashTable.at(hash_key).unlockCell();
	
	return rec;
}


/**
 * Loads the database into the hashtable
 * @param fileName - the file name
 * @return - the number of records left.
 */
int populateHashTable(const string& fileName)
{	
	/* The record */
	record rec;
	
	/* Open the file */
	ifstream dbFile(fileName.c_str());
	
	/* Is the file open */
	if(!dbFile.is_open())
	{
		fprintf(stderr, "Could not open file %s\n", fileName.c_str());
		exit(-1);
	}
	
	
	/* Read the entire file */
	while(!dbFile.eof())
	{
		/* Read the id */
		dbFile >> rec.id;
		
		/* Make sure we did not hit the EOF */
		if(!dbFile.eof())
		{
			/* Read the first name and last name */
			dbFile >> rec.firstName >> rec.lastName;
						
			/* Add to hash table */
			addToHashTable(rec);	
		}
	}
	
	/* Close the file */
	dbFile.close();
}

/**
 * Gets ids to process from work list
 * @return - the id of record to look up, or
 * -1 if there is no work
 */
int getIdsToLookUp()
{
	/* The id of the looked up record */
	int id = -1;

	/* DONE: Check if idsToLookUpList (see global declaration) contains any
	 * requests (i.e., ids of requested records).  If so, then remove that ID and
	 * return it. Otherwise, return -1 indicating that the queue is empty. Please
	 * remember that multiple threads can access this queue. 
	 * TO DO: Please make sure that there are no race conditions. 
	 * You can use the globally declared mutex
	 * idsToLookUpListMutex. If you use a different mutex, please make sure you
	 * use the same mutex in addIdsToLookUp (or face race conditions).
	 */


	/* Lock list to prevent others from accessing */
	if(pthread_mutex_lock(&idsToLookUpListMutex) != 0) 
	{
		perror("pthread_mutex_lock");
		exit(-1);
	}
	/* critical section */
	if(!idsToLookUpList.empty())
	{
		id = idsToLookUpList.front();
		idsToLookUpList.pop_front();
	}

	/* release lock after saving */
	if(pthread_mutex_unlock(&idsToLookUpListMutex) != 0) 
	{
		perror("pthread_mutex_unlock");
		exit(-1);
	}
		
	return id;
}

/**
 * Add id of record to look up
 * @param id - the id to process
 */
void addIdsToLookUp(const int& id)
{
	/* Add the id of the requested record to the idsToLookUpList (globally declared).
	 * TODO: Please make sure there are no race conditions. Protect all accesses using the approach
	 * described in getIdsToLookUp().
	 */
	/* Lock list to prevent others from accessing */
	if(pthread_mutex_lock(&idsToLookUpListMutex) != 0) 
	{
		perror("pthread_mutex_lock");
		exit(-1);
	}
	/* critical section */
	idsToLookUpList.push_back(id);

	/* release lock after saving */
	if(pthread_mutex_unlock(&idsToLookUpListMutex) != 0)
	{
		perror("pthread_mutex_unlock");
		exit(-1);
	}
}

/**
 * The thread function where the thread
 * services requests
 * @param thread argument
 */
void* fetcherThreadFunction(void* arg)
{
	/* TODO: The thread should stay in a loop until the user presses
	 * Ctrl-c.  The thread should check if there are any requests on the
	 * idsToLookUpList (you can use getIdsToLookUp() and if there are look up the
	 * record in the hashtable (you can use getHashTableRecord()) and send it back
	 * to the client (you can use sendRecord())
	 */

	while(!timeToExit)
	{
		record rec;
		rec.id = -1;
		/* Check for any requests on idsToLookUpList */
		int get_id = getIdsToLookUp();
		/* If there are no requests, return record id as -1 (save time this way?)*/
		if(get_id == -1) {
			rec.id = get_id;
			sendRecord(msqid, rec);
		}
		/* Returns the record. Will return record id as -1 if
		** record is not in the hashTable (performed in 
		** getHashTableRecord())
		**/
		else {
			rec = getHashTableRecord(get_id);
			sendRecord(msqid, rec);
		}
	}

}

/**
 * Creates threads that update the database
 * with randomly generated records
 */
void createInserterThreads()
{
	
	/* TODO: Create NUM_INSERTERS number of threads that
	 * will be constantly inserting records to the hash table.
	 * Threads can use addNewRecords() for that.
	 */
	/* vector<pthread_t> threadsIds(NUM_INSERTERS)*/
	for(int threadId = 0; threadId < NUM_INSERTERS; ++threadId) {
		if(pthread_create(&threadsIds.at(threadId), NULL, addNewRecords, NULL) != 0)
		{
			perror("pthread_create");
			exit(-1);
		}
	}
}

/**
 * Creates the specified number of fetcher threads
 * @param numFetchers - the number of fetcher threads
 */
void createFetcherThreads(const int& numFetchers)
{
	/* TODO: Create numFetchers threads that call fetcherThreadFunction().
	 * These threads will be responsible for responding to client requests
	 * with records looked up in the hashtable.
	 */
	/* The number of fetcher threads: vector<pthread_t> fetcherThreadIds */
	fetcherThreadIds.resize(numFetchers);

	for(int threadId = 0; threadId < numFetchers; ++threadId) {
		if(pthread_create(&fetcherThreadIds.at(threadId), NULL, fetcherThreadFunction, NULL) != 0)
		{
			perror("pthread_create");
			exit(-1);
		}
	}
}

/**
 * Called by parent thread to process incoming messages
 */
void processIncomingMessages()
{
	/* TODO: This function will be called by the parent thread.
	 * It will wait to recieve requests from the client on the
	 * message queue (you can use recvMessage() and message queue was
	 * already created for you (msqid is the id; see main()).
	 */
	
	/* Wait to receive requests */
	while(!timeToExit)
	{
		message msg;
		msg.messageType = CLIENT_TO_SERVER_MSG;
		recvMessage(msqid, msg, msg.messageType);
		addIdsToLookUp(msg.id);
	}
}

/**
 * Generates a random record
 * @return - a random record
 */
record generateRandomRecord()
{
	/* A record */
	record rec;
		
	/* Generate a random id */
	rec.id = rand() % NUMBER_OF_HASH_CELLS;	
	
	/* Add the fake first name and last name */
	rec.firstName = "Random";
	rec.lastName = "Record";
	
	return rec;
}

/**
 * Threads inserting new records to the database
 * @param arg - some argument (unused)
 */
void* addNewRecords(void* arg)
{	
	/* TODO: This function will be called by the threads responsible for
	 * performing random generation and insertion of new records into the
	 * hashtable.
	 */
	while(!timeToExit)
	{
		record rec;
		rec = generateRandomRecord();

		int key = rec.id % NUMBER_OF_HASH_CELLS;
		
		hashTable.at(key).lockCell();
		/*critical section*/
		hashTable.at(key).recordList.push_back(rec);
		hashTable.at(key).unlockCell();
	}

}

/**
 * Tells all threads to exit and waits until they exit
 * @param numThreads - the number of threads that retrieve
 * records from the hash table.
 */
void waitForAllThreadsToExit()
{
	/* TODO: Wait for all inserter threads and fetcher threads to join */
	for(int fetchId = 0; fetchId < fetcherThreadIds.size(); ++fetchId) {
		/* Create a thread */
		if(pthread_join(fetcherThreadIds[fetchId], NULL) != 0)
		{
			perror("pthread_create");
			exit(-1);
		}
	}
	for(int insertId = 0; insertId < NUM_INSERTERS; ++insertId) {
		/* Create a thread */
		if(pthread_join(threadsIds[insertId], NULL) != 0)
		{
			perror("pthread_create");
			exit(-1);
		}
	}
		
}



int main(int argc, char** argv)
{
	/**
 	 * Check the command line
 	 */
	if(argc < 2)
	{
		fprintf(stderr, "USAGE: %s <DATABASE FILE NAME>\n", argv[0]);
		exit(-1);
	}
	
	/* Install a signal handler */
	if(signal(SIGINT, cleanUp) == SIG_ERR)
	{
		perror("signal");
		exit(-1);
	}
		
	/* Populate the hash table */
	populateHashTable(argv[1]);
	
	/* Get the number of threads */
	numThreads = atoi(argv[2]);
	
	/* Use a random file and a random character to generate
	 * a unique key. Same parameters to this function will 
	 * always generate the same value. This is how multiple
	 * processes can connect to the same queue.
	 */
	key_t key = ftok("/bin/ls", 'O');
	
	/* Connect to the message queue; Completed for you */
	msqid = createMessageQueue(key);	
	
	
	/* Instantiate a message buffer */
	message msg;
	
	/* Create the threads */
	createFetcherThreads(numThreads);				
	
	//  Create the inserter threads 
	createInserterThreads();
		
	// // /* Process incoming requests */
	processIncomingMessages();	
	fprintf(stderr, "Here");	

	/* Terminate all threads */
	waitForAllThreadsToExit();	
	
	// /* Free the mutex used for protecting the work queue */
	if(pthread_mutex_destroy(&idsToLookUpListMutex) != 0)
		perror("pthread_mutex_destroy");
		
		
	return 0;
}
