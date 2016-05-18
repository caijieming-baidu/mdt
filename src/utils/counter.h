// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_COMMON_COUNTER_H_
#define  MDT_COMMON_COUNTER_H_

#include <stdio.h>
#include <pthread.h>
#include <map>
#include <iostream>

#include "atomic.h"
#include "timer.h"

namespace mdt {

class Counter {
public:
    Counter() : val_(0) {}
    int64_t Add(int64_t v) {
        return atomic_add64(&val_, v) + v;
    }
    int64_t Sub(int64_t v) {
        return atomic_add64(&val_, -v) - v;
    }
    int64_t Inc() {
        return atomic_add64(&val_, 1) + 1;
    }
    int64_t Dec() {
        return atomic_add64(&val_, -1) - 1;
    }
    int64_t Get() {
        return val_;
    }
    int64_t Set(int64_t v) {
        return atomic_swap64(&val_, v);
    }
    int64_t Clear() {
        return atomic_swap64(&val_, 0);
    }

private:
    volatile int64_t val_;
};

class AutoCounter {
public:
    AutoCounter(Counter* counter, const char* msg1, const char* msg2 = NULL)
        : counter_(counter),
          msg1_(msg1),
          msg2_(msg2) {
        start_ = timer::get_micros();
        counter_->Inc();
    }
    ~AutoCounter() {
        int64_t end = timer::get_micros();
        if (end - start_ > 5000000) {
            int64_t t = (end - start_) / 1000000;
            if (!msg2_) {
                fprintf(stderr, "%s [AutoCounter] %s hang for %ld s\n",
                    timer::get_curtime_str().data(), msg1_, t);
            } else {
                fprintf(stderr, "%s [AutoCounter] %s %s hang for %ld s\n",
                    timer::get_curtime_str().data(), msg1_, msg2_, t);
            }
        }
        counter_->Dec();
    }

private:
    Counter* counter_;
    int64_t start_;
    const char* msg1_;
    const char* msg2_;
};

struct CounterMap {
public:
    pthread_spinlock_t lock;
    std::map<std::string, Counter> kv;
    std::string cur_key;

    int64_t result_num;
    std::map<std::string, Counter> scan_kv;
    std::map<std::string, Counter>::iterator scan_it;

public:
    CounterMap() {
        cur_key = "";
        result_num = 100;
        pthread_spin_init(&lock, PTHREAD_PROCESS_SHARED);
    }
    ~CounterMap() {
        pthread_spin_destroy(&lock);
        kv.clear();
        scan_kv.clear();
    }

    int64_t Inc(const std::string& key) {
        int64_t res;
        pthread_spin_lock(&lock);
        Counter& c = kv[key];
        res = c.Inc();
        pthread_spin_unlock(&lock);
        return res;
    }

    int64_t Add(const std::string& key, int64_t val) {
        int64_t res;
        pthread_spin_lock(&lock);
        Counter& c = kv[key];
        res = c.Add(val);
        pthread_spin_unlock(&lock);
        return res;
    }

    int64_t Set(const std::string& key, int64_t val) {
        int64_t res;
        pthread_spin_lock(&lock);
        Counter& c = kv[key];
        res = c.Set(val);
        pthread_spin_unlock(&lock);
        return res;
    }

    // thread not safe
    int64_t ScanAndDelete(std::string* key, Counter* val, bool del = true) {
        if (scan_kv.size() > 0) {
            if (scan_it == scan_kv.end()) {
                scan_kv.clear();
            } else {
                *key = scan_it->first;
                val->Set((scan_it->second).Get());
                ++scan_it;
                return 0;
            }
        }

        pthread_spin_lock(&lock);
        int64_t i = 0;
        std::map<std::string, Counter>::iterator it;
        if (cur_key == "") {
            it = kv.begin();
        } else {
            it = kv.find(cur_key);
            if (it != kv.end()) {
                ++it;
            }
        }
        for (; (it != kv.end()) && (i < result_num); i++) {
            cur_key = it->first;
            Counter& c = scan_kv[cur_key];
            c.Set((it->second).Get());
            if (del) {
                it = kv.erase(it);
            } else {
                ++it;
            }
        }
        pthread_spin_unlock(&lock);

        if (scan_kv.size() > 0) {
            scan_it = scan_kv.begin();
            *key = scan_it->first;
            val->Set((scan_it->second).Get());
            ++scan_it;
            return 0;
        } else {
            cur_key.clear();
        }
        return -1;
    }
};

} // namespace mdt

#endif  // MDT_COMMON_COUNTER_H_
