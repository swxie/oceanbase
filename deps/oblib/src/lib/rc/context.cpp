/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB
#include "lib/rc/context.h"
#include "lib/lock/ob_mutex.h"
#include "lib/rc/ob_rc.h"
#include "lib/coro/co_var.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace lib {
RLOCAL(bool, ContextTLOptGuard::enable_tl_opt);

__MemoryContext__& __MemoryContext__::root()
{
  static __MemoryContext__ *root = nullptr;
  if (OB_UNLIKELY(nullptr == root)) {
    static lib::ObMutex mutex;
    lib::ObMutexGuard guard(mutex);
    if (nullptr == root) {
      ContextParam param;
      param.set_properties(ADD_CHILD_THREAD_SAFE | ALLOC_THREAD_SAFE)
          .set_parallel(4)
          .set_mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_ROOT_CONTEXT, ObCtxIds::DEFAULT_CTX_ID);
      // ObMallocAllocator to design a non-destroy mode
      const static int static_id =
        StaticInfos::get_instance().add(__FILENAME__, __LINE__, __FUNCTION__);
      __MemoryContext__ *tmp = new (std::nothrow) __MemoryContext__(false, DynamicInfo(), nullptr, param, static_id);
      abort_unless(tmp != nullptr);
      int ret = tmp->init();
      abort_unless(OB_SUCCESS == ret);
      root = tmp;
    }
  }
  return *root;
}

MemoryContext &MemoryContext::root()
{
  static MemoryContext root(&__MemoryContext__::root());
  return root;
}

} // end of namespace lib
} // end of namespace oceanbase
