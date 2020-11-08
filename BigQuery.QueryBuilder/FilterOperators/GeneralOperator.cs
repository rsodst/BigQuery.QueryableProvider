using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using BigQuery.QueryBuilder.Enums;
using BigQuery.QueryBuilder.Utils;

namespace BigQuery.QueryBuilder
{
    public class GeneralOperator<TLValue, TRValue> : OperatorBase
    {
        private static readonly Dictionary<OperatorAliasEnum, string> ConditionMapping =
            new Dictionary<OperatorAliasEnum, string>
            {
                [OperatorAliasEnum.Equal] = "=",
                [OperatorAliasEnum.NotEqual] = "!=",
                [OperatorAliasEnum.More] = ">",
                [OperatorAliasEnum.Less] = "<",
                [OperatorAliasEnum.MoreOrEqual] = ">=",
                [OperatorAliasEnum.LessOrEqual] = "<=",
                [OperatorAliasEnum.Like] = "LIKE"
            };

        public GeneralOperator(Expression<Func<TLValue, TRValue>> lValue, OperatorAliasEnum condition, TRValue rValue)
        {
            Filter = ($"{ReflectionUtils.GetPropertyName(lValue)} {ConditionMapping[condition]} \"{rValue}\"");
        }
    }
}