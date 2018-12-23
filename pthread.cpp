#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>


/**
 * The function called by the thread
 * @param args - pointer to the thread local data
 */
void* threadFunc(void* args)
{
	
	/* Print the thread id */
	fprintf(stderr, "Hi, I am thread %u\n", (unsigned int)pthread_self());
}

int main(int argc, char** argv)
{

	/* Check the arguments */
	if(argc < 2)
	{
		fprintf(stderr, "USAGE: %s <NUMBER OF THREADS>\n", argv[0]);
		exit(-1);
	}
	
	/* Grab the number of threads from the command line */
	int numberOfThreads = atoi(argv[1]);
	
	/* Array of thread variables */
	pthread_t threads[numberOfThreads];
	
	/* Lets create the threads */
	for(int threadNum = 0; threadNum < numberOfThreads; ++threadNum)
	{
		/* Create the thread */
		if(pthread_create(&threads[threadNum], NULL, threadFunc, NULL) < 0)
		{
			perror("pthread_create");
			exit(-1);
		}
	}
	
	/* Wait for all threads to finish */
	for(int threadNum = 0; threadNum < numberOfThreads; ++threadNum)
	{
		pthread_join(threads[threadNum], NULL);
	}
	
	return 0;
}

