using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace BigQuery.Orm
{
    public enum BigQueryConditionEnum
    {
        Equal,
        NotEqual,
        More,
        Less,
        MoreOrEqual,
        LessOrEqual,
    }

    public abstract class GenericBigQueryPredicate<TEntity>
    {
        public abstract string Compile();
    }
    
    public class BigQueryPredicate<TEntity> : GenericBigQueryPredicate<TEntity>
    {
        private readonly string _query;

        private BigQueryPredicate(string query)
        {
            _query = query;
        }
        
        public static GenericBigQueryPredicate<TEntity> And(GenericBigQueryPredicate<TEntity> lvalue, GenericBigQueryPredicate<TEntity> rvalue)
        {
            return new BigQueryPredicate<TEntity>($"({lvalue.Compile()} and {rvalue.Compile()})");
        }
        
        public static GenericBigQueryPredicate<TEntity> Or<TEntity>(GenericBigQueryPredicate<TEntity> lvalue, GenericBigQueryPredicate<TEntity> rvalue)
        {
            return new BigQueryPredicate<TEntity>($"({lvalue.Compile()} or {rvalue.Compile()})");
        }

        public override string Compile()
        {
            return _query;
        }
        
        public static implicit operator string(BigQueryPredicate<TEntity> predicate) => predicate.Compile();
    }
    
    public class BigQueryPredicate<TLValue,TRValue> : GenericBigQueryPredicate<TLValue>
    {
        private readonly string _attributeName;
        private readonly TRValue _rValue;
        private readonly BigQueryConditionEnum _condition;
        
        public BigQueryPredicate(Expression<Func<TLValue,TRValue>> lValue, BigQueryConditionEnum condition, TRValue rValue)
        {
            _attributeName = ReflectionExtensions.GetPropertyName(lValue);
            _condition = condition;
            _rValue = rValue;
        }
        
        private static readonly Dictionary<BigQueryConditionEnum, string> ConditionMapping =
            new Dictionary<BigQueryConditionEnum, string>
            {
                [BigQueryConditionEnum.Equal] = "=",
                [BigQueryConditionEnum.NotEqual] = "!=",
                [BigQueryConditionEnum.More] = ">",
                [BigQueryConditionEnum.Less] = "<",
                [BigQueryConditionEnum.MoreOrEqual] = ">=",
                [BigQueryConditionEnum.LessOrEqual] = "<=",
            };

        public override string Compile()
        {
            return $"{_attributeName} {ConditionMapping[_condition]} \"{_rValue}\"";
        }
    }
    
    public class BigQueryPredicates<TEntity>
    {
        public static BigQueryPredicate<TEntity, TRValue> Less<TRValue>(Expression<Func<TEntity,TRValue>> lvalue, TRValue rvalue)
        {
            return new BigQueryPredicate<TEntity, TRValue>(lvalue, BigQueryConditionEnum.Less, rvalue);
        }
        
        public static BigQueryPredicate<TEntity, TRValue> LessOrEqual<TRValue>(Expression<Func<TEntity,TRValue>> lvalue, TRValue rvalue)
        {
            return new BigQueryPredicate<TEntity, TRValue>(lvalue, BigQueryConditionEnum.LessOrEqual, rvalue);
        }
        
        public static BigQueryPredicate<TEntity, TRValue> More<TRValue>(Expression<Func<TEntity,TRValue>> lvalue, TRValue rvalue)
        {
            return new BigQueryPredicate<TEntity, TRValue>(lvalue, BigQueryConditionEnum.More, rvalue);
        }
        
        public static BigQueryPredicate<TEntity, TRValue> MoreOrEqual<TRValue>(Expression<Func<TEntity,TRValue>> lvalue, TRValue rvalue)
        {
            return new BigQueryPredicate<TEntity, TRValue>(lvalue, BigQueryConditionEnum.MoreOrEqual, rvalue);
        }
        
        public static BigQueryPredicate<TEntity, TRValue> Equal<TRValue>(Expression<Func<TEntity,TRValue>> lvalue, TRValue rvalue)
        {
            return new BigQueryPredicate<TEntity, TRValue>(lvalue, BigQueryConditionEnum.Equal, rvalue);
        }
        
        public static BigQueryPredicate<TEntity, TRValue> NotEqual<TRValue>(Expression<Func<TEntity,TRValue>> lvalue, TRValue rvalue)
        {
            return new BigQueryPredicate<TEntity, TRValue>(lvalue, BigQueryConditionEnum.NotEqual, rvalue);
        }
    }
}