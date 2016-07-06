// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef _MDT_MAIL_H
#define _MDT_MAIL_H

#include <string>

namespace mdt {
class Mail {
public:
    int SendMail(const std::string& to, const std::string& from,
                 const std::string& subject, const std::string& message);
};
}
#endif
