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

//用于缓存中间结果的结构
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

//动态采样服务
class ObOptSampleService {

  friend class ObOptSampleServicePointer;

public:
  ObOptSampleService(sql::ObExecContext* exec_ctx);

  ~ObOptSampleService();

  int init();

  // use dynamic sample to get selectivity of exprs on base table
  // @param est_sel_info[in]
  // @param quals[in]
  // @param selectivity[out]
  int get_single_table_selectivity(const sql::ObEstSelInfo& est_sel_info, const ObIArray<sql::ObRawExpr*>& quals, double& selectivity);
  
  //添加了连接参数
  int get_join_table_selectivity(const sql::ObEstSelInfo& est_sel_info, const ObIArray<sql::ObRawExpr*>& quals, double& selectivity, sql::ObJoinType join_type, const sql::ObRelIds* left_rel_ids,
    const sql::ObRelIds* right_rel_ids, const double left_row_count, const double right_row_count);

private:
  bool inited_;
  int64_t tenant_id_;
  sql::ObExecContext* exec_ctx_;
  common::ObMySQLProxy* mysql_proxy_;

  ObSEArray<ObStringSelPair, 8> cache_;

  //引用计数相关，用于管理内存
  int ref_;

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

  //内部接口-去参数化

  int print_where_clause(const ObIArray<sql::ObRawExpr*>& input_quals, const sql::ParamStore* params, sql::ObRawExprFactory& expr_factory, ObSqlString &output);

  int calc_const_expr(sql::ObRawExpr* &expr, const sql::ParamStore* params, sql::ObRawExprFactory& expr_factory);

  //内部接口-拼sql
  
  int generate_single_table_innersql(const sql::TableItem* cur_table_item, const ObSqlString &where_clause, double percent,
      int seed, ObSqlString& sql);

  int generate_join_table_innersql(const ObSEArray<sql::TableItem*, 3>& cur_table_items, int index, const ObSqlString &where_buffer, double percent,
      int seed, ObSqlString& sql);
  
  //内部接口-取数

  int fetch_dynamic_stat_int(ObSqlString& sql, int &count);
  
  int fetch_dynamic_stat_double(ObSqlString& sql, double& selectivity);

  int cast_number_to_double(const number::ObNumber& src_val, double& dst_val);

};  // end of class ObOptSampleService

// sample_service的智能指针，用于在多个改写的optctx间共享缓存，并管理动态内存
class ObOptSampleServicePointer {
public:
  ObOptSampleServicePointer(sql::ObExecContext* exec_ctx)
  {
    service_ = (exec_ctx == NULL ? NULL : new ObOptSampleService(exec_ctx));
  }

  ObOptSampleServicePointer() : service_(NULL)
  {}

  ObOptSampleServicePointer(const ObOptSampleServicePointer& other)
  {
    service_ = other.get_service();
    if (service_ != NULL)
      service_->add_ref();
  }

  ObOptSampleServicePointer& operator=(const ObOptSampleServicePointer& other)
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

  ~ObOptSampleServicePointer()
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
};  // end of class ObOptSampleServicePointer

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_OPT_SAMPLE_SERVICE_H_ */