using System.Collections.Generic;
using System.Threading.Tasks;

namespace BigQuery.QueryBuilder.Operations
{
    public class DeleteOperation<TEntity>
    {
        private readonly BigQueryContext _context;
        private readonly string _whereCondition;
        
        public DeleteOperation(OperatorBase operatorBase)
        {
            _whereCondition = operatorBase.Filter;
        }

        public Task DeleteAsync()
        {
            var sql = GetSql();
            
            return _context.BigQueryClient.ExecuteQueryAsync(sql, null);
        }
        
        // private
        
        private string GetSql()
        {
            var result = $"DELETE FROM {typeof(TEntity).Name}";

            result += $"\nWHERE {_whereCondition}";

            return result;
        }
    }
}