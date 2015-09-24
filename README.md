###we use it to synchronous data in cluster
* you can add the value for the same key
* then synchronous the value to all the memcached or redis in the cluster
* this is a light tools and no memory leak 
* this tools build need libmemcached
* compile ==> first:ldconfig  second:gcc memcount.c -o memcount -lpthread -lmemcached
