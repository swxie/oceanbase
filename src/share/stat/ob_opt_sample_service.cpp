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

ObOptSampleService::ObOptSampleService(ObExecContext* ctx) : inited_(false), tenant_id_(1), exec_ctx_(ctx)
{
  init();
}

int ObOptSampleService::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Exec context is null", K(ret));
  } else if (OB_ISNULL(mysql_proxy_ = exec_ctx_->get_sql_proxy())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Faile to get mysql proxy", K(ret));
  } else if (OB_FAIL(count_map_.create(11, ObModIds::OB_HASH_BUCKET_PLAN_STAT, ObModIds::OB_HASH_NODE_PLAN_STAT)) ||
             expr_map_.create(11, ObModIds::OB_HASH_BUCKET_PLAN_STAT, ObModIds::OB_HASH_NODE_PLAN_STAT)) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Faile to init dynamic sample service", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObOptSampleService::get_expr_selectivity(
    const ObEstSelInfo& est_sel_info, const ObRawExpr* qual, double& selectivity_output)
{
  int ret = OB_SUCCESS;
  double selectivity = 0.0;
  ObExprPtr ptr(qual);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql service has not initialized.", K(ret));
  } else if (OB_ISNULL(qual)) {  //检查qual合法性
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument of NULL pointer", K(qual), K(ret));
  } else if (!OB_ISNULL(expr_map_.get(ptr))) {  //检查缓存
    selectivity = *(expr_map_.get(ptr));
  } else {
    //检查innersql
    const ObSQLSessionInfo* session = est_sel_info.get_session_info();
    if (OB_ISNULL(session)) {
      LOG_WARN("Failed to get session info", K(session), K(ret));
    } else if (session->is_inner()) {
      // do nothing
    } else {
      //开始动态采样
      tenant_id_ = session->get_effective_tenant_id();
      ObRawExpr* print_qual;
      ObRawExprFactory expr_factory(const_cast<ObIAllocator&>(est_sel_info.get_allocator()));
      const ParamStore* params = est_sel_info.get_params();
      ObSEArray<uint64_t, 3> cur_table_ids;                     //存储from中表的id
      ObSEArray<TableItem*, 3> cur_table_items;                 //存储from中items
      char where_buffer[100];                                   //缓冲区，用于读取where从句
      int64_t pos_where = 0;                                    //缓冲区的结束位置
      ObRawExprPrinter printer(where_buffer, 100, &pos_where);  // where从句打印
      const ObDMLStmt* stmt = est_sel_info.get_stmt();
      if (OB_ISNULL(stmt)) {
        LOG_WARN("Failed to get statment", K(stmt), K(ret));
      } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, qual, print_qual, COPY_REF_DEFAULT))) {  //深拷贝qual
        LOG_WARN("failed to copy raw expr", K(ret));
      } else if (OB_FAIL(calc_const_expr(print_qual, params, expr_factory))){
        
      } else if (OB_FAIL(printer.do_print(print_qual, T_WHERE_SCOPE))) {
        LOG_WARN("failed to get where clause", K(ret));
      } else if (OB_UNLIKELY(pos_where >= 99)) {  //待定，考虑缓存区是否被占满
        LOG_INFO("Where clause is too long for buffer", K(ret));
      } else if (qual->is_const_expr() || qual->has_flag(IS_CALCULABLE_EXPR) || qual->has_flag(CNT_AGG) ||
                 qual->is_column_ref_expr()) {
        LOG_INFO("No need to use dynamic sample on this type of expr", K(where_buffer), K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(qual, cur_table_ids))) {
        LOG_WARN("extract table ids failed", K(ret));
      } else {
        bool flag = true;
        int max_index = -1;
        double max_value = 0;
        //查找每个基表的统计信息,并确定对哪个表采样
        for (int i = 0; i < cur_table_ids.count(); i++) {
          uint64_t temp_table_id = cur_table_ids[i];
          TableItem* temp_table_item = const_cast<TableItem*>(stmt->get_table_item_by_id(temp_table_id));
          //不能完整拼出
          if (temp_table_item == NULL) {
            LOG_WARN("fail to get table item", K(ret));
            flag = false;
            break;
          }
          if (!temp_table_item->is_basic_table()) {
            LOG_INFO("temp table item is not basic table", K(ret));
            flag = false;
            break;
          }
          cur_table_items.push_back(temp_table_item);
          uint64_t temp_table_ref_id = temp_table_item->ref_id_;
          if (temp_table_ref_id == common::OB_INVALID_ID) {
            LOG_WARN("fail to get ref table", K(ret));
            flag = false;
            break;
          }
          if (OB_ISNULL(count_map_.get(temp_table_ref_id))) {
            //处理逻辑
            double count = 0;
            double temp_count = 0;
            if (OB_FAIL(const_cast<ObEstAllTableStat&>(est_sel_info.get_table_stats())
                            .get_rows(temp_table_id, temp_count, count))) {
              LOG_WARN("fail to fetch table size", K(ret));
              flag = false;
              break;
            } else if (count == 0) {
              count = static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
            }
            if (OB_FAIL(count_map_.set_refactored(temp_table_ref_id, count))) {
              LOG_WARN("fail to insert table count", K(ret));
              flag = false;
              break;
            }
          }
          double row_count = *(count_map_.get(temp_table_ref_id));
          if (row_count > max_value) {
            max_value = row_count;
            max_index = i;
          }
        }
        //拼sql，迭代sql查询
        if (flag == true) {
          double percent = 10000.0 / max_value * 100;
          if (percent < 0.000001) {
            percent = 0.000001;
          } else if (percent >= 100) {
            percent = 100;
          }
          double result_1 = 0, result_2 = 0;
          if (OB_FAIL(fetch_expr_stat(cur_table_items, max_index, where_buffer, percent, 1, result_1))) {
            // do nothing
          } else if (percent < 100 &&
                     OB_FAIL(fetch_expr_stat(cur_table_items, max_index, where_buffer, percent, 2, result_2))) {
            // do nothing
          } else {
            selectivity = percent < 100 ? (result_1 + result_2) / 2 : result_1;
            int seed = 2;
            while (selectivity == 0 && percent < 16) {
              percent *= 2;
              seed++;
              if (OB_FAIL(fetch_expr_stat(cur_table_items, max_index, where_buffer, percent, seed, selectivity)))
                break;
            }
            if (OB_FAIL(expr_map_.set_refactored(ptr, selectivity))) {
              LOG_WARN("fail to load selectiviti into cache", K(ret));
            }
          }
        }
      }
    }
  }
  if (selectivity != 0){
    selectivity_output = selectivity;//存疑，是否在为0时使用默认配置
  }
  return ret;
}

int ObOptSampleService::fetch_expr_stat(ObSEArray<sql::TableItem*, 3>& cur_table_items, int index, char* where_buffer,
    double percent, int seed, double& selectivity)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ret = generate_innersql(cur_table_items, index, where_buffer, percent, seed, sql);
  ret = fetch_dynamic_stat(sql, selectivity);
  return ret;
}

int ObOptSampleService::generate_innersql(ObSEArray<sql::TableItem*, 3>& cur_table_items, int index, char* where_buffer,
    double percent, int seed, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.append_fmt("select (count(case when (%s) then 1 else null end)/count(*)) as result from ", where_buffer);
  for (int i = 0; i < cur_table_items.count(); i++) {
    TableItem* temp_table_item = cur_table_items.at(i);
    ObString database_name = temp_table_item->database_name_;
    sql.append_fmt("%.*s.%.*s", database_name.length(), database_name.ptr(), 
      temp_table_item->table_name_.length(), temp_table_item->table_name_.ptr());
    if (index == i && percent < 100 && percent >= 0.00001) {
      sql.append_fmt(" sample(%f) seed(%d)", percent, seed);
    }
    if (!temp_table_item->alias_name_.empty()) {
      sql.append(" ");
      sql.append(temp_table_item->alias_name_);
    }
    if (i != cur_table_items.count() - 1)
      sql.append(", ");
    else
      sql.append(";");
  }
  return ret;
}

int ObOptSampleService::fetch_dynamic_stat(ObSqlString& sql, double& selectivity)
{
  int ret = OB_SUCCESS;
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
  } else {
    LOG_WARN("succeed to cast number to double", K(src_val), K(dst_val));
  }
  return ret;
}

int ObOptSampleService::calc_const_expr(ObRawExpr* &expr, const ParamStore* params, ObRawExprFactory& expr_factory){
  int ret = OB_SUCCESS;
  const ObConstRawExpr* const_expr = NULL;
  ObObj obj;
  bool need_check = false;
  if (OB_ISNULL(expr) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(expr), K(params), K(ret));
  } else if (expr->is_const_expr()){
    const_expr = static_cast<const ObConstRawExpr*>(expr);
    if (T_QUESTIONMARK == const_expr->get_expr_type() && !const_expr->has_flag(IS_EXEC_PARAM)){
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
      if (OB_FAIL(calc_const_expr(expr->get_param_expr(idx), params, expr_factory)))
        LOG_WARN("fail to calc const expr");
    }
  }
  return ret;
}

}  // end of namespace common
}  // end of namespace oceanbase