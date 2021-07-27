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

namespace sql{
  class ObRawExpr;
  class ObEstSelInfo;
  class ObRawExprPrinter;
  class ObExecContext;
} //end of namespace sql

namespace common{
class ObServerConfig;
class ObMySQLProxy;

struct ObExprPtr {
  const sql::ObRawExpr* ptr;
  ObExprPtr(const sql::ObRawExpr* ptr_){
    ptr = ptr_;
  }
  ObExprPtr() : ptr(NULL){
  };
  bool operator==(const ObExprPtr right) const{
    return this->ptr == right.ptr;
  }
  uint64_t hash() const{
    return calc_hash(reinterpret_cast<uint64_t>(ptr));
  }
};

class ObOptSampleService{
  public:
  ObOptSampleService(sql::ObExecContext* exec_ctx);
  ~ObOptSampleService(){}
  int init();

  // use dynamic sample to get selectivity of an expr
  // @param est_sel_info[in]
  // @param qual[in]
  // @param selectivity[out]
  int get_expr_selectivity(const sql::ObEstSelInfo& est_sel_info, const sql::ObRawExpr* qual, 
    double& selectivity);

  private:
  bool inited_;
  int64_t tenant_id_;
  sql::ObExecContext* exec_ctx_;
  common::ObMySQLProxy* mysql_proxy_;
  common::hash::ObHashMap<uint64_t, double> count_map_;
  common::hash::ObHashMap<ObExprPtr, double> expr_map_;

  //大接口
  int calc_const_expr(sql::ObRawExpr* &expr, const sql::ParamStore* params, sql::ObRawExprFactory& expr_factory);
  int fetch_expr_stat(ObSEArray<sql::TableItem*, 3> &cur_table_items, int index, char* where_buffer, double percent, int seed, double &selectivity);//获得谓词行数的接口
  //内部接口
  int generate_innersql(ObSEArray<sql::TableItem*, 3> &cur_table_items, int index, char* where_buffer, double percent, int seed, ObSqlString& sql);//拼sql的接口
  int fetch_dynamic_stat(ObSqlString& sql, double &selectivity);
  int cast_number_to_double(const number::ObNumber &src_val, double &dst_val);
  
};// end of class ObOptSampleService

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_OPT_SAMPLE_SERVICE_H_ */