using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace BigQuery.QueryBuilder.Utils
{
    public static class ReflectionUtils
    {
        public static IEnumerable<PropertyInfo> GetProperties<T>()
        {
            return typeof(T).GetProperties();
        }

        public static string GetPropertyName<TL, TR>(Expression<Func<TL, TR>> expression)
        {
            var expressionNameExtractors = new List<Func<Expression, string>>
            {
                expressionBody =>
                {
                    if (!(expressionBody is MemberExpression))
                    {
                        return string.Empty;
                    }

                    var memberExpression = (MemberExpression) expressionBody;

                    return memberExpression.Member.Name;
                },
                expressionBody =>
                {
                    if (!(expressionBody is MethodCallExpression))
                    {
                        return string.Empty;
                    }

                    var methodCallExpression = (MethodCallExpression) expressionBody;

                    return methodCallExpression.Method.Name;
                },
                expressionBody =>
                {
                    if (!(expressionBody is UnaryExpression))
                    {
                        return string.Empty;
                    }

                    var unaryExpression = (UnaryExpression) expressionBody;

                    if (!(unaryExpression.Operand is MethodCallExpression))
                    {
                        return ((MemberExpression) unaryExpression.Operand).Member.Name;
                    }

                    var methodExpression = (MethodCallExpression) unaryExpression.Operand;

                    return methodExpression.Method.Name;
                }
            };

            foreach (var name in
                expressionNameExtractors
                    .Select(extractor => extractor.Invoke(expression.Body)).Where(name => !string.IsNullOrEmpty(name)))
            {
                return name;
            }

            return string.Empty;
        }
    }
}