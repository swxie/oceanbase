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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RAWTOHEX_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RAWTOHEX_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase {
namespace sql {
class ObExprRawtohex : public ObStringExprOperator {
public:
  explicit ObExprRawtohex(common::ObIAllocator& alloc);
  virtual ~ObExprRawtohex();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result1(common::ObObj& result, const common::ObObj& text, common::ObExprCtx& expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_rawtohex_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRawtohex);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RAWTOHEX_ */
