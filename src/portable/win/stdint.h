// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// MSVC didn't ship with this file until the 2010 version.

#ifndef MDT_PORT_WIN_STDINT_H_
#define MDT_PORT_WIN_STDINT_H_

#if !defined(_MSC_VER)
#error This file should only be included when compiling with MSVC.
#endif

// Define C99 equivalent types.
typedef signed char           int8_t;
typedef signed short          int16_t;
typedef signed int            int32_t;
typedef signed long long      int64_t;
typedef unsigned char         uint8_t;
typedef unsigned short        uint16_t;
typedef unsigned int          uint32_t;
typedef unsigned long long    uint64_t;

#endif  // STORAGE_LEVELDB_PORT_WIN_STDINT_H_