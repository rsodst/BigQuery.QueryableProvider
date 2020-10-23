using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using BigQuery.QueryBuilder.Utils;

namespace BigQuery.QueryBuilder
{
    public enum StatementConditionEnum
    {
        Equal,
        NotEqual,
        More,
        Less,
        MoreOrEqual,
        LessOrEqual
    }

    public abstract class GenericStatement
    {
        public abstract string Compile();
    }

    public class Statement : GenericStatement
    {
        private readonly string _query;

        private Statement(string query)
        {
            _query = query;
        }

        public static GenericStatement And(GenericStatement lvalue, GenericStatement rvalue)
        {
            return new Statement($"({lvalue.Compile()} AND {rvalue.Compile()})");
        }

        public static GenericStatement Or(GenericStatement lvalue, GenericStatement rvalue)
        {
            return new Statement($"({lvalue.Compile()} OR {rvalue.Compile()})");
        }

        public override string Compile()
        {
            return _query;
        }

        public static implicit operator string(Statement predicate)
        {
            return predicate.Compile();
        }
    }

    public class Statement<TLValue, TRValue> : GenericStatement
    {
        private static readonly Dictionary<StatementConditionEnum, string> ConditionMapping =
            new Dictionary<StatementConditionEnum, string>
            {
                [StatementConditionEnum.Equal] = "=",
                [StatementConditionEnum.NotEqual] = "!=",
                [StatementConditionEnum.More] = ">",
                [StatementConditionEnum.Less] = "<",
                [StatementConditionEnum.MoreOrEqual] = ">=",
                [StatementConditionEnum.LessOrEqual] = "<="
            };

        private readonly string _attributeName;
        private readonly StatementConditionEnum _condition;
        private readonly TRValue _rValue;

        public Statement(Expression<Func<TLValue, TRValue>> lValue, StatementConditionEnum condition,
            TRValue rValue)
        {
            _attributeName = ReflectionUtils.GetPropertyName(lValue);
            _condition = condition;
            _rValue = rValue;
        }

        public override string Compile()
        {
            return $"{_attributeName} {ConditionMapping[_condition]} \"{_rValue}\"";
        }
    }

    public class StatementPredicate<TEntity>
    {
        public static Statement<TEntity, TRValue> Less<TRValue>(Expression<Func<TEntity, TRValue>> lvalue,
            TRValue rvalue)
        {
            return new Statement<TEntity, TRValue>(lvalue, StatementConditionEnum.Less, rvalue);
        }

        public static Statement<TEntity, TRValue> LessOrEqual<TRValue>(
            Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new Statement<TEntity, TRValue>(lvalue, StatementConditionEnum.LessOrEqual, rvalue);
        }

        public static Statement<TEntity, TRValue> More<TRValue>(Expression<Func<TEntity, TRValue>> lvalue,
            TRValue rvalue)
        {
            return new Statement<TEntity, TRValue>(lvalue, StatementConditionEnum.More, rvalue);
        }

        public static Statement<TEntity, TRValue> MoreOrEqual<TRValue>(
            Expression<Func<TEntity, TRValue>> lvalue, TRValue rvalue)
        {
            return new Statement<TEntity, TRValue>(lvalue, StatementConditionEnum.MoreOrEqual, rvalue);
        }

        public static Statement<TEntity, TRValue> Equal<TRValue>(Expression<Func<TEntity, TRValue>> lvalue,
            TRValue rvalue)
        {
            return new Statement<TEntity, TRValue>(lvalue, StatementConditionEnum.Equal, rvalue);
        }

        public static Statement<TEntity, TRValue> NotEqual<TRValue>(Expression<Func<TEntity, TRValue>> lvalue,
            TRValue rvalue)
        {
            return new Statement<TEntity, TRValue>(lvalue, StatementConditionEnum.NotEqual, rvalue);
        }
    }
}