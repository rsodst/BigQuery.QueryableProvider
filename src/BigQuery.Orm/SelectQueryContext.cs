using System;
using System.Collections.Generic;

namespace BigQuery.Orm
{
    public interface ISelectQueryContext<TEntity>
    {
        string Query { get; }
        
        Func<string, IEnumerable<TEntity>> ExecuteSql { get; set; }
    }

    public class SimpleSelectQueryContext<TEntity> : ISelectQueryContext<TEntity>
    {
        public SimpleSelectQueryContext(string from, IEnumerable<string> properties)
        {
            Query = $"SELECT {string.Join(",", properties)} FROM {from}";
        }

        public SimpleSelectQueryContext(string from)
        {
            Query += $"SELECT * FROM {from}";
        }

        public string Query { get; }
        
        public Func<string, IEnumerable<TEntity>> ExecuteSql { get; set; }
    }
    
    public class WhereSelectQueryContext<TEntity> : ISelectQueryContext<TEntity>
    {
        public string Query { get; }
        public Func<string, IEnumerable<TEntity>> ExecuteSql { get; set; }

        public WhereSelectQueryContext(ISelectQueryContext<TEntity> context, GenericBigQueryPredicate<TEntity> filter)
        {
            Query = $"{context.Query} WHERE {filter.Compile()}";
        }
        
        public WhereSelectQueryContext(ISelectQueryContext<TEntity> context, string filter)
        {
            Query = $"{context.Query} {filter}";
        }
    }
}