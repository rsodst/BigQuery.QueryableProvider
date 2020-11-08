using System;
using System.Linq.Expressions;
using BigQuery.QueryBuilder.Enums;

namespace BigQuery.QueryBuilder
{
    public class OperatorFactory<TEntity>
    {
        public GeneralOperator<TEntity, TRValue> Less<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.Less, rvalue);
        }

        public GeneralOperator<TEntity, TRValue> LessOrEqual<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.LessOrEqual, rvalue);
        }

        public GeneralOperator<TEntity, TRValue> More<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.More, rvalue);
        }

        public GeneralOperator<TEntity, TRValue> MoreOrEqual<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.MoreOrEqual, rvalue);
        }

        public GeneralOperator<TEntity, TRValue> Equal<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.Equal, rvalue);
        }

        public GeneralOperator<TEntity, TRValue> NotEqual<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.NotEqual, rvalue);
        }

        public GeneralOperator<TEntity, TRValue> Like<TRValue>(Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new GeneralOperator<TEntity, TRValue>(lvalue, OperatorAliasEnum.Like, rvalue);
        }
    }
}