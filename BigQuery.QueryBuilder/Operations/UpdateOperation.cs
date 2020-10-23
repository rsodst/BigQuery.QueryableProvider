using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace BigQuery.QueryBuilder.Operations
{
    public class UpdateOperation<TEntity>
    {
        private readonly string _whereCondition = null;
        
        public UpdateOperation(GenericStatement statement)
        {
            _whereCondition = statement.Compile();
        }
        
        public void From (params (Expression<Func<TEntity,object>>, object)[] updateExpressions)
        {
        }
        
        public void From (TEntity entity)
        {
        }

        public string GetSql()
        {
            var result = $"DELETE FROM {typeof(TEntity).Name}";

            result += $"\nWHERE {_whereCondition}";

            return result;
        }
    }
}