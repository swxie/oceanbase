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

#ifndef _OB_OPT_SAMPLE_SERVICE_H_
#define _OB_OPT_SAMPLE_SERVICE_H_

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/hash/ob_hash.h"

namespace oceanbase {

namespace sql {
class ObRawExpr;
class ObEstSelInfo;
class ObRawExprPrinter;
class ObExecContext;
}  // end of namespace sql

namespace common {
class ObServerConfig;
class ObMySQLProxy;

struct ObStringSelPair {
  ObStringSelPair() : string_(""), sel_(0)
  {}
  ObStringSelPair(const ObString string, double sel) : string_(string), sel_(sel)
  {}
  ~ObStringSelPair()
  {}
  bool operator==(const ObStringSelPair& rhs) const
  {
    return string_ == rhs.string_;
  }
  TO_STRING_KV(K(string_), K(sel_));
  ObString string_;
  double sel_;  // selectiviy of expr

  ObStringSelPair& operator=(const ObStringSelPair& other)
  {
    sel_ = other.sel_;
    string_.assign_ptr(other.string_.ptr(), other.string_.length());
    return *this;
  }
};

class ObOptSampleService {
  public:
  ObOptSampleService(sql::ObExecContext* exec_ctx);
  ~ObOptSampleService();

  int get_ref()
  {
    return ref_;
  }

  int add_ref()
  {
    ref_++;
    return ref_;
  }

  int sub_ref()
  {
    ref_--;
    return ref_;
  }

  int init();

  // use dynamic sample to get selectivity of an expr
  // @param est_sel_info[in]
  // @param qual[in]
  // @param selectivity[out]
  int get_expr_selectivity(const sql::ObEstSelInfo& est_sel_info, const sql::ObRawExpr* qual, double& selectivity);

  private:
  bool inited_;
  int64_t tenant_id_;
  sql::ObExecContext* exec_ctx_;
  common::ObMySQLProxy* mysql_proxy_;
  
  ObSEArray<ObStringSelPair, 8> cache;
  int ref_;

  //大接口
  int calc_const_expr(sql::ObRawExpr*& expr, const sql::ParamStore* params, sql::ObRawExprFactory& expr_factory);
  int fetch_expr_stat(ObSEArray<sql::TableItem*, 3>& cur_table_items, int index, char* where_buffer, double percent,
      int seed, double& selectivity);  //获得谓词行数的接口
  //内部接口
  int generate_innersql(ObSEArray<sql::TableItem*, 3>& cur_table_items, int index, char* where_buffer, double percent,
      int seed, ObSqlString& sql);  //拼sql的接口
  int fetch_dynamic_stat(ObSqlString& sql, double& selectivity);
  int cast_number_to_double(const number::ObNumber& src_val, double& dst_val);

};  // end of class ObOptSampleService

//sample_service的代理，用于在多个改写的optctx间共享缓存
class ObOptSampleServiceAgent {
  public:
  ObOptSampleServiceAgent(sql::ObExecContext* exec_ctx)
  {
    service_ = new ObOptSampleService(exec_ctx);
  }

  ObOptSampleServiceAgent() : service_(NULL)
  {}

  ObOptSampleServiceAgent(const ObOptSampleServiceAgent& other)
  {
    service_ = other.get_service();
    if (service_ != NULL)
      service_->add_ref();
  }

  ObOptSampleServiceAgent& operator=(const ObOptSampleServiceAgent& other)
  {
    if (other.get_service() == service_) {
      // do nothing
    } else {
      if (service_ != NULL && service_->sub_ref() == 0)
        delete service_;
      service_ = other.get_service();
      if (service_ != NULL)
        service_->add_ref();
    }
    return *this;
  }

  ~ObOptSampleServiceAgent()
  {
    if (service_ != NULL && service_->sub_ref() == 0) {
      delete service_;
    }
  }

  ObOptSampleService* get_service() const
  {
    return service_;
  }

  private:
  ObOptSampleService* service_;
};  // end of class ObOptSampleServiceAgent

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_OPT_SAMPLE_SERVICE_H_ */