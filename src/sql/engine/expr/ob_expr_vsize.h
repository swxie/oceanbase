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

#ifndef OB_EXPR_VSIZE_H
#define OB_EXPR_VSIZE_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprVsize : public ObFuncExprOperator {
public:
  explicit ObExprVsize(common::ObIAllocator& alloc);
  virtual ~ObExprVsize();
  virtual int calc_result1(common::ObObj& result, const common::ObObj& input, common::ObExprCtx& expr_ctx) const;
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;

  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_vsize_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVsize);

  bool is_blob_type(const common::ObObj& input) const;
};
}  // namespace sql
}  // namespace oceanbase

#endif
