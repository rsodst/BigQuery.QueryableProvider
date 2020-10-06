using System.Collections.Generic;
using System.Linq;

namespace BigQuery.Orm
{
    public static class SelectExtensions
    {
        public static WhereSelectQueryContext<TEntity> Where<TEntity>(this SimpleSelectQueryContext<TEntity> context, GenericBigQueryPredicate<TEntity> filter)
        {
            return new WhereSelectQueryContext<TEntity>(context, filter)
            {
                ExecuteSql = context.ExecuteSql
            };
        }
        
        public static TEntity First<TEntity>(this ISelectQueryContext<TEntity> context)
        {
            return context.ExecuteSql($"{context.Query} LIMIT 1").FirstOrDefault();
        }
        
        public static IEnumerable<TEntity> ToList<TEntity>(this ISelectQueryContext<TEntity> context)
        {
            return context.ExecuteSql($"{context.Query}");
        }
    }
}