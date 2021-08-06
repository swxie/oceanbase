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

#define USING_LOG_PREFIX COMMON
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/utility.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

#include "sql/optimizer/ob_opt_est_sel.h"

#include "ob_opt_sample_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace common {

ObOptSampleService::ObOptSampleService(ObExecContext* ctx) : inited_(false), tenant_id_(1), exec_ctx_(ctx), ref_(1)
{}

ObOptSampleService::~ObOptSampleService()
{
  int sample_time = cache_.count();
  if (sample_time > 0)
    LOG_INFO("Finish dynamic sample service", K(sample_time));
}

int ObOptSampleService::init()
{
  int ret = OB_SUCCESS;
  LOG_INFO("Use dynamic sample");
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Exec context is null", K(ret));
  } else if (OB_ISNULL(mysql_proxy_ = exec_ctx_->get_sql_proxy())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Faile to get mysql proxy", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObOptSampleService::get_single_table_selectivity(
    const sql::ObEstSelInfo& est_sel_info, const ObIArray<sql::ObRawExpr*>& quals, double& selectivity_output)
{
  int ret = OB_SUCCESS;
  double selectivity = 0.0;
  if (!inited_ && OB_FAIL(init())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not initialized.", K(ret));
  } else {
    //检查innersql
    const ObSQLSessionInfo* session = est_sel_info.get_session_info();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("Failed to get session info", K(session), K(ret));
    } else if (session->is_inner()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("innersql can not be sample", K(ret));
    } else {
      tenant_id_ = session->get_effective_tenant_id();
      ObSqlString where_clause;
      ObSEArray<uint64_t, 3> cur_table_ids;
      TableItem* cur_table_item = NULL;
      ObRawExprFactory expr_factory(const_cast<ObIAllocator&>(est_sel_info.get_allocator()));
      const ParamStore* params = est_sel_info.get_params();
      const ObDMLStmt* stmt = est_sel_info.get_stmt();
      //进行必要的检查和表抽取
      for (int i = 0; i < quals.count(); i++) {
        ObRawExpr* expr = quals.at(i);
        if (expr == NULL) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid argument of NULL pointer", K(expr), K(ret));
        } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(expr, cur_table_ids))) {
          LOG_WARN("extract table ids failed", K(ret));
        }
      }
      //开始动态采样
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("Failed to get statment", K(stmt), K(ret));
      } else if (ret != OB_SUCCESS) {
        // do nothing
      } else if (cur_table_ids.count() != 1) {
        ret = OB_INVALID_ARGUMENT; 
        //LOG_WARN("call the wrong func", K(ret), K(cur_table_ids.count()));
      } else {
        cur_table_item = const_cast<TableItem*>(stmt->get_table_item_by_id(cur_table_ids[0]));
        uint64_t cur_table_ref_id = OB_INVALID_ID;
        double count = 0;
        double temp_count = 0;
        if (cur_table_item == NULL) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("fail to get table item", K(ret));
        } else if (!cur_table_item->is_basic_table()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_INFO("temp table item is not basic table", K(ret));
        } else if (OB_FAIL(const_cast<ObEstAllTableStat&>(est_sel_info.get_table_stats())
                               .get_rows(cur_table_ids[0], temp_count, count))) {
          LOG_WARN("fail to fetch table size", K(ret));
        } else if (OB_FAIL(print_where_clause(quals, params, expr_factory, where_clause))) {
          //参数化导致
        } else {
          if (count == 0) {
            count = static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
          }
          double percent = 10000.0 / count * 100;
          if (percent < 0.000001) {
            percent = 0.000001;
          } else if (percent >= 100) {
            percent = 100;
          }
          double result_1 = 0, result_2 = 0;
          ObSqlString query;
          if (OB_FAIL(generate_single_table_innersql(cur_table_item, where_clause, percent, 1, query)) ||
              OB_FAIL(fetch_dynamic_stat(query, result_1))) {
            LOG_WARN("fail to sample first time", K(ret));
          } else if (percent < 100 &&
                     (OB_FAIL(generate_single_table_innersql(cur_table_item, where_clause, percent, 2, query)) ||
                         OB_FAIL(fetch_dynamic_stat(query, result_2)))) {
            LOG_WARN("fail to sample second time", K(ret));
          } else {
            selectivity = percent < 100 ? (result_1 + result_2) / 2 : result_1;
            int seed = 2;
            while (selectivity == 0 && percent < 16) {
              percent *= 2;
              seed++;
              if (OB_FAIL(generate_single_table_innersql(cur_table_item, where_clause, percent, seed, query)) ||
                  OB_FAIL(fetch_dynamic_stat(query, selectivity))) {
                LOG_WARN("failed to sample", K(ret));
              }
            }
          }
        }
      }
    }
  }
  if (ret == OB_SUCCESS) {
    selectivity_output = selectivity;  //成功时赋值
    LOG_WARN("yingnan debug 1", K(selectivity), K(selectivity_output));
  }
  return ret;
}

int ObOptSampleService::print_where_clause(const ObIArray<sql::ObRawExpr*>& input_quals, const sql::ParamStore* params,
    sql::ObRawExprFactory& expr_factory, ObSqlString& output)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < input_quals.count(); i++) {
    ObRawExpr* print_qual;
    char where_buffer[1000];                                   //缓冲区，用于读取where从句
    int64_t pos_where = 0;                                     //缓冲区的结束位置
    ObRawExprPrinter printer(where_buffer, 1000, &pos_where);  // where从句打印
    if (OB_FAIL(
            ObRawExprUtils::copy_expr(expr_factory, input_quals.at(i), print_qual, COPY_REF_DEFAULT))) {  //深拷贝qual
      LOG_WARN("failed to copy raw expr", K(ret));
    } else if (OB_FAIL(calc_const_expr(print_qual, params, expr_factory))) {
      LOG_WARN("failed to calc const expr", K(ret));
    } else if (OB_FAIL(printer.do_print(print_qual, T_WHERE_SCOPE))) {
      LOG_WARN("failed to get where clause", K(ret));
    }
    if (ret == OB_SUCCESS) {
      if (i == 0)
        output.append_fmt("%.*s", static_cast<int>(pos_where), where_buffer);
      else
        output.append_fmt(" and %.*s", static_cast<int>(pos_where), where_buffer);
    } else
      break;
  }
  return ret;
}

int ObOptSampleService::calc_const_expr(ObRawExpr*& expr, const ParamStore* params, ObRawExprFactory& expr_factory)
{
  int ret = OB_SUCCESS;
  const ObConstRawExpr* const_expr = NULL;
  ObObj obj;
  bool need_check = false;
  if (OB_ISNULL(expr) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(expr), K(params), K(ret));
  } else if (expr->is_const_expr()) {
    const_expr = static_cast<const ObConstRawExpr*>(expr);
    if (T_QUESTIONMARK == const_expr->get_expr_type() && const_expr->has_flag(IS_EXEC_PARAM)) {
      ret = OB_ERROR;
      LOG_INFO("don't use dynamic sample on exec param", K(ret));
    } else if (T_QUESTIONMARK == const_expr->get_expr_type()) {
      if (OB_FAIL(ObSQLUtils::get_param_value(const_expr->get_value(), *params, obj, need_check)))
        LOG_WARN("get param value error", K(ret));
      else {
        ObConstRawExpr* c_expr = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(obj.get_type()), c_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else {
          c_expr->set_value(obj);
          expr = c_expr;
        }
      }
    }
  } else {
    int64_t num = expr->get_param_count();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < num; ++idx) {
      if (OB_FAIL(calc_const_expr(expr->get_param_expr(idx), params, expr_factory))) {
        // do nothing
      }
    }
  }
  return ret;
}

int ObOptSampleService::generate_single_table_innersql(
    const sql::TableItem* cur_table_item, const ObSqlString& where_clause, double percent, int seed, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  const ObString where_string = where_clause.string();
  sql.append_fmt("select (count(case when (%.*s) then 1 else null end)/count(*)) as result from ",
      where_string.length(),
      where_string.ptr());
  ObString database_name = cur_table_item->database_name_;
  if (percent < 100)
    sql.append_fmt("%.*s.%.*s sample(%f) seed(%d)",
        database_name.length(),
        database_name.ptr(),
        cur_table_item->table_name_.length(),
        cur_table_item->table_name_.ptr(),
        percent,
        seed);
  else
    sql.append_fmt("%.*s.%.*s",
        database_name.length(),
        database_name.ptr(),
        cur_table_item->table_name_.length(),
        cur_table_item->table_name_.ptr());
  if (!cur_table_item->alias_name_.empty()) {
    sql.append(" ");
    sql.append(cur_table_item->alias_name_);
  }
  sql.append(";");
  return ret;
}

int ObOptSampleService::fetch_dynamic_stat(ObSqlString& sql, double& selectivity)
{
  int ret = OB_SUCCESS;
  int64_t* index_ptr = NULL;
  ObStringSelPair target_pair(sql.string(), 0.0);
  if (has_exist_in_array(cache_, target_pair, index_ptr)) {
    selectivity = cache_.at(*index_ptr).sel_;
  } else {
    DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id_, sql.ptr()))) {
        COMMON_LOG(WARN, "execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "fail to execute ", "sql", sql.ptr(), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          COMMON_LOG(WARN, "result next failed, ", K(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else {
        number::ObNumber src_val;
        EXTRACT_NUMBER_FIELD_MYSQL(*result, result, src_val);
        if (OB_FAIL(cast_number_to_double(src_val, selectivity))) {
          LOG_WARN("Failed to cast number to double");
        } else {
          target_pair.sel_ = selectivity;
        }
        if (OB_FAIL(add_var_to_array_no_dup(cache_, target_pair))) {
          LOG_WARN("fail to load selectiviti into cache_", K(ret));
        } else {
          // do nothing
          LOG_WARN("yingnan debug", K(sql), K(selectivity));
        }
      }
    }
  }
  return ret;
}

int ObOptSampleService::cast_number_to_double(const number::ObNumber& src_val, double& dst_val)
{
  int ret = OB_SUCCESS;
  ObObj src_obj;
  ObObj dest_obj;
  src_obj.set_number(src_val);
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
  ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
  if (OB_FAIL(ObObjCaster::to_type(ObDoubleType, cast_ctx, src_obj, dest_obj))) {
    LOG_WARN("failed to cast number to double type", K(ret));
  } else if (OB_FAIL(dest_obj.get_double(dst_val))) {
    LOG_WARN("failed to get double", K(ret));
  }
  return ret;
}

int ObOptSampleService::get_join_table_selectivity(const sql::ObEstSelInfo& est_sel_info,
    const ObIArray<sql::ObRawExpr*>& quals, double& selectivity_output, sql::ObJoinType join_type,
    const sql::ObRelIds* left_rel_ids, const sql::ObRelIds* right_rel_ids, double left_row_count,
    double right_row_count, const common::ObIArray<sql::ObRawExpr*>& left_quals,
    const common::ObIArray<sql::ObRawExpr*>& right_quals)
{
  int ret = OB_SUCCESS;
  double selectivity = 0.0;
  if (!inited_ && OB_FAIL(init())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not initialized.", K(ret));
  } else {
    //检查innersql
    const ObSQLSessionInfo* session = est_sel_info.get_session_info();
    if (OB_ISNULL(session)) {
      LOG_WARN("Failed to get session info", K(session), K(ret));
    } else if (session->is_inner()) {
      // do nothing
    } else {
      tenant_id_ = session->get_effective_tenant_id();
      ObSqlString where_clause, join_clause, left_clause, right_clause;
      ObSEArray<uint64_t, 3> cur_table_ids;
      TableItem* cur_table_item = NULL;
      ObRawExprFactory expr_factory(const_cast<ObIAllocator&>(est_sel_info.get_allocator()));
      const ParamStore* params = est_sel_info.get_params();
      const ObDMLStmt* stmt = est_sel_info.get_stmt();
      //进行必要的检查和表抽取
      for (int i = 0; i < quals.count(); i++) {
        ObRawExpr* expr = quals.at(i);
        if (expr == NULL) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid argument of NULL pointer", K(expr), K(ret));
        } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(expr, cur_table_ids))) {
          LOG_WARN("extract table ids failed", K(ret));
        }
      }
      //开始动态采样
      if (OB_ISNULL(stmt)) {
        LOG_WARN("Failed to get statment", K(stmt), K(ret));
      } else if (ret != OB_SUCCESS) {
        // do nothing
      } else if (cur_table_ids.count() != 2) {
        ret = OB_INVALID_ARGUMENT;
        //LOG_WARN("call the wrong func", K(ret), K(cur_table_ids.count()));
      } else if (OB_FAIL(print_where_clause(quals, params, expr_factory, join_clause)) ||
                 OB_FAIL(print_where_clause(left_quals, params, expr_factory, left_clause)) ||
                 OB_FAIL(print_where_clause(right_quals, params, expr_factory, right_clause))) {
      } else {
        double max_count = 0;
        int index = 0;
        ObSEArray<TableItem*, 3> cur_table_items;
        //检查表
        for (int i = 0; (ret == OB_SUCCESS) && i < cur_table_ids.count(); i++) {
          cur_table_item = const_cast<TableItem*>(stmt->get_table_item_by_id(cur_table_ids[i]));
          double count = 0;
          double temp_count = 0;
          if (cur_table_item == NULL) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("fail to get table item", K(ret));
          } else if (!cur_table_item->is_basic_table()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_INFO("temp table item is not basic table", K(ret));
          } else if (OB_FAIL(const_cast<ObEstAllTableStat&>(est_sel_info.get_table_stats())
                                 .get_rows(cur_table_ids[i], temp_count, count))) {
            LOG_WARN("fail to fetch table size", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(cur_table_items, cur_table_item))) {
            LOG_WARN("failed to get table item", K(ret));
          }
          if (count == 0) {
            count = static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
          }
          if (count > max_count) {
            max_count = count;
            index = i;
          }
        }
        if (ret == OB_SUCCESS) {
          where_clause.append(join_clause.string());
          if (left_clause.string() != "") {
            where_clause.append(" and ");
            where_clause.append(left_clause.string());
          }
          if (right_clause.string() != "") {
            where_clause.append(" and ");
            where_clause.append(right_clause.string());
          }
          double percent = 10000.0 / max_count * 100;
          if (percent < 0.000001) {
            percent = 0.000001;
          } else if (percent >= 100) {
            percent = 100;
          }
          double result_1 = 0, result_2 = 0;
          ObSqlString query;
          if (OB_FAIL(generate_join_table_innersql(cur_table_items, index, where_clause, percent, 1, query)) ||
              OB_FAIL(fetch_dynamic_stat(query, result_1, left_row_count, right_row_count))) {
            LOG_WARN("fail to sample first time", K(ret));
          } else if (percent < 100 &&
                     (OB_FAIL(generate_join_table_innersql(cur_table_items, index, where_clause, percent, 2, query)) ||
                         OB_FAIL(fetch_dynamic_stat(query, result_2, left_row_count, right_row_count)))) {
            LOG_WARN("fail to sample second time", K(ret));
          } else {
            selectivity = percent < 100 ? (result_1 + result_2) / 2 : result_1;
            int seed = 2;
            while (selectivity == 0 && percent < 16) {
              percent *= 2;
              seed++;
              if (OB_FAIL(generate_join_table_innersql(cur_table_items, index, where_clause, percent, seed, query)) ||
                  OB_FAIL(fetch_dynamic_stat(query, selectivity, left_row_count, right_row_count))) {
                LOG_WARN("failed to sample", K(ret));
              }
            }
          }
        }
      }
    }
  }
  if (ret == OB_SUCCESS) {
    selectivity_output = selectivity;  //成功时赋值
    LOG_WARN("yingnan debug 1", K(selectivity), K(selectivity_output));
  }
  return ret;
}

int ObOptSampleService::generate_join_table_innersql(const ObSEArray<TableItem*, 3>& cur_table_items, int index,
    const ObSqlString& where_buffer, double percent, int seed, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  sql.append("select count(*) as result from ");
  for (int i = 0; i < cur_table_items.count(); i++) {
    TableItem* cur_table_item = cur_table_items.at(i);
    ObString database_name = cur_table_item->database_name_;
    if (i == index && percent < 100) {
      sql.append_fmt("%.*s.%.*s sample(%f) seed(%d)",
          database_name.length(),
          database_name.ptr(),
          cur_table_item->table_name_.length(),
          cur_table_item->table_name_.ptr(),
          percent,
          seed);
    } else {
      sql.append_fmt("%.*s.%.*s",
          database_name.length(),
          database_name.ptr(),
          cur_table_item->table_name_.length(),
          cur_table_item->table_name_.ptr());
    }
    if (!cur_table_item->alias_name_.empty()) {
      sql.append(" ");
      sql.append(cur_table_item->alias_name_);
    }
    if (i != cur_table_items.count() - 1)
      sql.append(",");
    sql.append(" ");
  }
  const ObString where_string = where_buffer.string();
  sql.append_fmt("where %.*s;", where_string.length(), where_string.ptr());
  return ret;
}

int ObOptSampleService::fetch_dynamic_stat(
    ObSqlString& sql, double& selectivity, double left_row_count, double right_row_count)
{
  int ret = OB_SUCCESS;
  int64_t* index_ptr = NULL;
  ObStringSelPair target_pair(sql.string(), 0.0);
  if (has_exist_in_array(cache_, target_pair, index_ptr)) {
    selectivity = cache_.at(*index_ptr).sel_;
  } else {
    DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id_, sql.ptr()))) {
        COMMON_LOG(WARN, "execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "fail to execute ", "sql", sql.ptr(), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          COMMON_LOG(WARN, "result next failed, ", K(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else {
        int src_val = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "result", src_val, int);
        target_pair.sel_ = static_cast<double>(src_val) / (left_row_count * right_row_count);
        selectivity = target_pair.sel_;
        if (OB_FAIL(add_var_to_array_no_dup(cache_, target_pair))) {
          LOG_WARN("fail to load selectiviti into cache_", K(ret));
        } else {
          LOG_WARN("yingnan debug", K(sql), K(selectivity));
        }
      }
    }
  }
  return ret;
}
}  // end of namespace common
}  // end of namespace oceanbase