#include <queue>
#include <unistd.h>
#include "ftrace/logger.h"
#include "util/mutex.h"

#include "proto/ftrace_test.pb.h"

typedef void* (*ThreadFunc)(void*);
struct ThreadQueue {
    mdt::Mutex mu;
    mdt::CondVar cv;
    bool stop;
    std::queue<void*> queue;
    ThreadFunc func;

    ThreadQueue(ThreadFunc user_func)
        : cv(&mu) {
        stop = false;
        func = user_func;
    }

    void Enqueue(void* arg) {
        mdt::MutexLock mutex(&mu);
        queue.push(arg);
        cv.Signal();
    }

    void Disable() {
        mdt::MutexLock mutex(&mu);
        stop = true;
        cv.Signal();
    }

    void Wait() {
        mdt::MutexLock mutex(&mu);
        cv.Wait();
    }

    void* Dequeue() {
        mdt::MutexLock mutex(&mu);
        if (queue.size() == 0)
            return NULL;
        void* req = queue.front();
        queue.pop();
        return req;
    }
};

void* thread_func(void* arg) {
    ThreadQueue* queue = (ThreadQueue*)arg;
    while (1) {
        void* req = NULL;
        while ((req = queue->Dequeue()) == NULL || queue->stop) {
            if (queue->stop) return NULL;
            queue->Wait();
        }
        queue->func(req);
    }
    return NULL;
}

void* FuncC(void* arg) {
    mdt::DetachAndOpenTrace((uint64_t)arg);
    mdt::KvLog(12, "key#FuncC", "[%s] %012u, kv", __func__, 7878);

    std::cout << "step into " << __func__ << std::endl;
    std::cout << "value: " << (char*)arg;
    std::cout << "step out " << __func__ << std::endl;

    mdt::ReleaseTrace();
    return NULL;
}
ThreadQueue commit_queue(FuncC);

void* FuncB(void* arg) {
    mdt::DetachAndOpenTrace((uint64_t)arg);
    mdt::KvLog(12, "key#FuncB", "[%s] %012u, kv", __func__, 7878);

    mdt::AttachTrace((uint64_t)arg);
    std::cout << "step into " << __func__ << std::endl;
    sleep(1);
    std::cout << "step out " << __func__ << std::endl;

    mdt::ReleaseTrace();

    commit_queue.Enqueue(arg);
    return NULL;
}
ThreadQueue handle_queue(FuncB);

void* FuncA(void* arg) {
    mdt::DetachAndOpenTrace((uint64_t)arg);
    mdt::KvLog(12, "key#FuncA", "[%s] %012u, kv", __func__, 7878);

    mdt::AttachTrace((uint64_t)arg);
    std::cout << "step into " << __func__ << std::endl;
    sleep(1);
    std::cout << "step out " << __func__ << std::endl;

    mdt::ReleaseTrace();

    handle_queue.Enqueue(arg);
    return NULL;
}
ThreadQueue prepare_queue(FuncA);

void TEST_case002() {
    ::mdt::galaxy::test::PodStat pod_stat;
    std::cout << "case002...\n";
    ::mdt::OpenProtoBufLog("mdt.galaxy.test", "PodStat");
    ::mdt::LogProtoBuf("id", &pod_stat);
    //::mdt::CloseProtoBufLog("mdt.galaxy.test", "PodStat");
}

void TEST_case003() {
    ::mdt::galaxy::test::PodStat pod_stat;
    std::cout << "case003...\n";
    pod_stat.set_id("88880000aaaa");
    pod_stat.set_jobid("ddddeeeeffff");
    pod_stat.set_cpu_used(80);
    pod_stat.set_mem_used(1000000000);
    pod_stat.set_cpu_quota(90);
    pod_stat.set_mem_quota(2000000000);
    pod_stat.set_dynamic_cpu_quota(85);
    pod_stat.set_dynamic_mem_quota(1500000000);
    ::mdt::OpenProtoBufLog("mdt.galaxy.test", "PodStat");
    ::mdt::LogProtoBuf("id", &pod_stat);
    //::mdt::CloseProtoBufLog("mdt.galaxy.test", "PodStat");
}

void TEST_case004() {
    std::cout << "test case4 ..., open 4 table, begin\n";
    ::mdt::OpenProtoBufLog("baidu.galaxy", "PodStat");
    ::mdt::OpenProtoBufLog("baidu.galaxy", "PodEvent");
    ::mdt::OpenProtoBufLog("baidu.galaxy", "JobStat");
    ::mdt::OpenProtoBufLog("baidu.galaxy", "JobEvent");
    std::cout << "test case4 ..., open 4 table, success\n";
}

int main(int ac, char* av[]) {
    ::mdt::InitTraceModule("../conf/ftrace.flag");

    // construct request
    char* ctx = new char[100];
    char* p = ctx;
    sprintf(p, "trace context[%012u] test\n", 10000);

    mdt::OpenTrace(0, 0, 0, "span name", "trace name");

    // enable work thread
    pthread_t p_tid, h_tid, c_tid;
    mdt::KvLog(12, "key#888", "[%s] %012u, kv", __func__, 7878);
    pthread_create(&p_tid, NULL, thread_func, &prepare_queue);
    pthread_create(&h_tid, NULL, thread_func, &handle_queue);
    pthread_create(&c_tid, NULL, thread_func, &commit_queue);

    mdt::AttachTrace((uint64_t)ctx);

    prepare_queue.Enqueue((void*)ctx);
    mdt::Log(12, "[%s]%08u, text %s", __func__, 7878, "xxxxxxxxx");
    mdt::Log(12, "[%s]%08u, ctx %s", __func__, 555, "=========");
    mdt::Log(12, "[%s]%08u", __func__, 111);
    mdt::KvLog(12, "4444444444444444", "[%s] %012u, kv", __func__, 7878);
    sleep(3);

    mdt::ReleaseTrace();

    // disable work thread
    prepare_queue.Disable();
    handle_queue.Disable();
    commit_queue.Disable();
    pthread_join(p_tid, NULL);
    pthread_join(h_tid, NULL);
    pthread_join(c_tid, NULL);
    delete ctx;

    // test case
    TEST_case003();
    TEST_case002();
    TEST_case004();
    return 0;
}

