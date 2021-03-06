#include <stdio.h>
#include <libmemcached/memcached.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#define MEMCOUNT_KEY_NUM 3
const size_t key_length[]= {2, 3, 3};
const char *keys[] = {"dd", "dd1", "dd2"};
const char *servers_addr[] = {"--SERVER=10.77.96.122:35920","--SERVER=10.77.96.122:35921","--SERVER=10.77.96.122:35922","--SERVER=10.77.96.122:35923", NULL};
const char *prefix = "_total";
const char ** init_prefix_keys();
int daemon_mode = 0;

void display(memcached_st ** servers, int count,const char **keys_result);
int use_daemon();
int get_options(int argc, char *const *argv);

int main(int argc, char *const *argv)
{
    int *sum = (int *)malloc(sizeof(int) * MEMCOUNT_KEY_NUM);
    memset(sum, 0, sizeof(int) * MEMCOUNT_KEY_NUM);
    const int count = (sizeof servers_addr) / sizeof(char *) - 1;
    char const ** pserver_addr = servers_addr;
    memcached_st **servers = (memcached_st **) malloc((sizeof (memcached_st *)) * count);
    memset(servers, 0, (sizeof (memcached_st *)) * count);
    memcached_return_t rc;
    memcached_result_st rst;
    const char * key = NULL;
    int index = 0;
    int i = 0;
    const char **keys_result = init_prefix_keys();
    char value_result[20] = "";
    
    get_options(argc, argv);

    if(daemon_mode){
        use_daemon();
    }

    for(; *pserver_addr != NULL; pserver_addr ++){
        servers[i] =  memcached(*pserver_addr, strlen(*pserver_addr));
        i++;
    }
    
    while(true){
        memset(sum, 0, sizeof(int) * MEMCOUNT_KEY_NUM);
        for(i = 0; i < count; i ++){
                rc = memcached_mget(servers[i], keys, key_length, MEMCOUNT_KEY_NUM);         
                if (rc != MEMCACHED_SUCCESS){
                    printf("%s ", memcached_strerror(servers[i], rc));
                }
            }

        for(i = 0; i < count; i ++){
            memcached_result_create(servers[i], &rst);
            while(memcached_fetch_result(servers[i], &rst, &rc)){
                key = memcached_result_key_value(&rst);    
                for(index = 0; index < MEMCOUNT_KEY_NUM; index ++){
                    if(strlen(key) != strlen(keys[index])){
                        continue;
                    }
                    if(strncmp(key, keys[index], strlen(keys[index])) == 0){
                        break;
                    }
                }    
                sum[index] = sum[index] + atoi(memcached_result_value(&rst));

                memcached_result_free(&rst);
                memcached_result_create(servers[i], &rst);
            }
            memcached_result_free(&rst);
        }

        for(i = 0; i < count; i++){
            for(index = 0; index < MEMCOUNT_KEY_NUM; index ++){
                sprintf(value_result, "%d", sum[index]);
                memcached_set(servers[i], keys_result[index], strlen(keys_result[index]), value_result, strlen(value_result), 0, 0); 
                memset(value_result, 0, sizeof value_result);   
            }
        }

        if(!daemon_mode){        
            display(servers, count, keys_result);
        }
        usleep(100 * 1000);
    }
    return 0;
}

const char ** init_prefix_keys()
{
    int index = 0;
    int buffsize = 0;
    int prefix_size =  strlen(prefix);
    char *buff = NULL, *pos = NULL;
    const char ** keys_result = malloc(sizeof(char *) * MEMCOUNT_KEY_NUM);
    memset(keys_result, 0, sizeof(char *) * MEMCOUNT_KEY_NUM);
    for(; index < MEMCOUNT_KEY_NUM; index ++){
        buffsize = strlen(keys[index]) + prefix_size + 5 + buffsize;
    }

    buff = (char *)malloc(buffsize);
    memset(buff, 0, buffsize);
    pos = buff;
    for(index = 0; index < MEMCOUNT_KEY_NUM; index ++){
        keys_result[index] = pos;
        memcpy(pos, keys[index], strlen(keys[index]));
        pos = pos + strlen(keys[index]);
        memcpy(pos, prefix, prefix_size);
        pos = pos + prefix_size;
        pos = pos + 2;
    }
    return keys_result;
}


void display(memcached_st ** servers, int count, const char **keys_result)
{
    memcached_return_t t;
    uint32_t flag;
    size_t size;
    char *s;
    printf("############################display all##################################\n");
    int i = 0, index = 0;
    for (i = 0; i < count; i++){
        printf("############################display mem %d##################################\n", i);
        for(index = 0; index < MEMCOUNT_KEY_NUM; index ++){
            s = memcached_get(servers[i], keys[index], strlen(keys[index]), &size, &flag, &t);
            printf("%s---->%s\n", keys[index], s);
            free(s);
         }

        for(index = 0; index < MEMCOUNT_KEY_NUM; index ++){
            s = memcached_get(servers[i], keys_result[index], strlen(keys_result[index]), &size, &flag, &t);
            printf("%s---->%s\n", keys_result[index], s);
            free(s);
        }
    }
}

int get_options(int argc, char *const *argv)
{
    char *p;
    int   i;

    for (i = 1; i < argc; i++) { 
        p = (u_char *) argv[i];
        if (*p++ != '-') {
            printf("invalid option: \"%s\"\n", argv[i]);
            return -1;
        }

        while (*p) {
            switch (*p++) {
                case 'd':
                    daemon_mode = 1;
                    break;
                default:
                    printf("invalid option: \"%c\"\n", *(p - 1));
                    return -1;
            }
        }
    }

    return 0;
}

int use_daemon()
{
    int  fd;
    switch (fork()) {
        case -1:
            printf("fork() failed!\n");
            return -1;
        case 0:
            break;
        default:
            exit(0);
    }

    if (setsid() == -1) {
        printf("setsid() failed\n");
        return -1;
    }

    umask(0);

    fd = open("/dev/null", O_RDWR);
    if (fd == -1) {
        printf("open(\"/dev/null\") failed\n");
        return -1;
    }

    if (dup2(fd, STDIN_FILENO) == -1) {
        printf("dup2(STDIN) failed\n");
        return -1;
    }

    if (dup2(fd, STDOUT_FILENO) == -1) {
        printf("dup2(STDOUT) failed\n");
        return -1;
    }

    if (fd > STDERR_FILENO) {
        if (close(fd) == -1) {
            printf("close() failed\n");
            return -1;
        }
    }

    return 0;
}

