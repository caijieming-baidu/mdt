#include <stdio.h>
#include <string.h>
#include <dlfcn.h>

#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>

#include <sys/time.h>
#include <sys/syscall.h>
#include <pthread.h>

#include <execinfo.h>

#if defined(RTLD_NEXT)
#  define REAL_LIBC RTLD_NEXT
#else
#  define REAL_LIBC ((void *) -1L)
#endif

#define FN(ptr, type, name, args) ptr = (type (*)args)dlsym (REAL_LIBC, name)

//#include <map>
//#include <string>
//std::map<int, std::string> Hook_checker;

#if 0
int open(const char *pathname, int flags) {
    static int (*func)(const char *, int);
    FN(func, int, "open", (const char *, int));
    int ret = (*func)(pathname, flags);

    char buf[30];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    snprintf(buf, 30,
        "%02d-%02d.%02d:%02d:%02d.%06d",
        t.tm_mon + 1,
        t.tm_mday,
        t.tm_hour,
        t.tm_min,
        t.tm_sec,
        (int)(tv.tv_usec));
    fprintf(stderr, "%s, %ld, open fd %d, %s\n", buf, syscall(__NR_gettid), ret, pathname);

    const size_t max_depth = 100;
    size_t stack_depth;
    void *stack_addrs[max_depth];
    char **stack_strings;

    stack_depth = backtrace(stack_addrs, max_depth);
    stack_strings = (char**)backtrace_symbols(stack_addrs, stack_depth);
    size_t i = 0;
    for (; i < stack_depth; i++) {
        fprintf(stderr, "%s, %ld, open fd %d, stack[%d] %s\n", buf, syscall(__NR_gettid), ret, i, stack_strings[i]);
    }
    free(stack_strings); // malloc()ed by backtrace_symbols

    return ret;
}
#endif
int open(const char *pathname, int flags, mode_t mode) {
    static int (*func)(const char *, int, mode_t);
    FN(func, int, "open", (const char *, int, mode_t));
    int ret = (*func)(pathname, flags, mode);

    char buf[30];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    snprintf(buf, 30,
        "%02d-%02d.%02d:%02d:%02d.%06d",
        t.tm_mon + 1,
        t.tm_mday,
        t.tm_hour,
        t.tm_min,
        t.tm_sec,
        (int)(tv.tv_usec));
    fprintf(stderr, "%s, %ld, open fd %d, %s\n", buf, syscall(__NR_gettid), ret, pathname);

#if 0
    const size_t max_depth = 100;
    size_t stack_depth;
    void *stack_addrs[max_depth];
    char **stack_strings;

    stack_depth = backtrace(stack_addrs, max_depth);
    stack_strings = (char**)backtrace_symbols(stack_addrs, stack_depth);
    size_t i = 0;
    for (; i < stack_depth; i++) {
        fprintf(stderr, "%s, %ld, open fd %d, stack[%d] %s\n", buf, syscall(__NR_gettid), ret, i, stack_strings[i]);
    }
    free(stack_strings); // malloc()ed by backtrace_symbols
#endif
    return ret;
}

int close(int fd) {
    static int (*func)(int);
    FN(func, int, "close", (int));

    char buf[30];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    snprintf(buf, 30,
        "%02d-%02d.%02d:%02d:%02d.%06d",
        t.tm_mon + 1,
        t.tm_mday,
        t.tm_hour,
        t.tm_min,
        t.tm_sec,
        (int)(tv.tv_usec));
    fprintf(stderr, "%s, %ld, close fd %d\n", buf, syscall(__NR_gettid), fd);

    const size_t max_depth = 100;
    size_t stack_depth;
    void *stack_addrs[max_depth];
    char **stack_strings;

    stack_depth = backtrace(stack_addrs, max_depth);
    stack_strings = (char**)backtrace_symbols(stack_addrs, stack_depth);
    size_t i = 0;
    for (; i < stack_depth; i++) {
      fprintf(stderr, "%s, %ld, close fd %d, stack[%d] %s\n", buf, syscall(__NR_gettid), fd, i, stack_strings[i]);
    }
    free(stack_strings); // malloc()ed by backtrace_symbols

    return (*func)(fd);
}


