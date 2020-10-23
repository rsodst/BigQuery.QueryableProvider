using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using BigQuery.QueryBuilder.Enums;
using BigQuery.QueryBuilder.Utils;

namespace BigQuery.QueryBuilder.Operations
{
    public class SelectOperation<TEntity> 
    {
        public OrderModeEnum OrderMode = OrderModeEnum.Asc;

        private readonly BigQueryContext _context;
        public readonly List<string> OrderAttributes = new List<string>();
        public readonly List<string> SelectedAttributes = new List<string>();
        public readonly List<string> WhereStatements = new List<string>();

        public SelectOperation(BigQueryContext context, params Expression<Func<TEntity, object>>[] selectors)
        {
            SelectedAttributes = selectors.Select(ReflectionUtils.GetPropertyName).ToList();
            _context = context;
        }

        public async Task<IEnumerable<TEntity>> SelectManyAsync()
        {
            var sql = GetSql();
            
            var job = await _context.BigQueryClient.CreateQueryJobAsync(sql, null);

            await job.PollUntilCompletedAsync();

            var result = await _context.BigQueryClient.GetQueryResultsAsync(job.Reference);

            var entities = new List<TEntity>();

            foreach (var row in result)
            {
                var entity = Activator.CreateInstance<TEntity>();

                foreach (var field in row.Schema.Fields)
                {
                    var prop = typeof(TEntity).GetProperty(field.Name);

                    prop?.SetValue(entity, BigQueryTypeMapper.MapFromBigQuery[prop.PropertyType](row[field.Name]));
                }

                entities.Add(entity);
            }

            return entities;
        }

        public async Task<TEntity> SelectSecondAsync() => (await SelectManyAsync()).FirstOrDefault();
        
        // private
        private string GetSql()
        {
            var result = $"SELECT {(SelectedAttributes.Any() ? string.Join(", ", SelectedAttributes) : "*")} " +
                         $"\nFROM {_context.CreateFromStatement<TEntity>()}";

            if (WhereStatements.Any())
            {
                result += $"\nWHERE {string.Join(" ", WhereStatements)}";
            }

            if (OrderAttributes.Any())
            {
                result += $"\nORDER BY {string.Join(", ", OrderAttributes)} {OrderMode}";
            }

            return result;
        }
    }

    public static class SelectOperationExtensions
    {
        public static SelectOperation<TEntity> OrderBy<TEntity>(this SelectOperation<TEntity> operation,
            params Expression<Func<TEntity, object>>[] orderByAttribute)
         
        {
            operation.OrderAttributes.Add(
                string.Join(", ", orderByAttribute.Select(ReflectionUtils.GetPropertyName).ToList()));

            return operation;
        }
        
        public static SelectOperation<TEntity> OrderByDesc<TEntity>(this SelectOperation<TEntity> operation,
            params Expression<Func<TEntity, object>>[] orderByAttribute)
        {
            operation.OrderAttributes.Add(
                string.Join(", ", orderByAttribute.Select(ReflectionUtils.GetPropertyName).ToList()));
            operation.OrderMode = OrderModeEnum.Desc;
            
            return operation;
        }

        public static Task<TEntity> First<TEntity>(this SelectOperation<TEntity> operation,
            GenericStatement statement)
        {
            operation.WhereStatements.Add(operation.WhereStatements.Any()
                ? $"AND ({statement.Compile()})"
                : $"{statement.Compile()}");

            return operation.SelectSecondAsync();
        }
        
        public static Task<IEnumerable<TEntity>> ToList<TEntity>(this SelectOperation<TEntity> operation,
            GenericStatement statement, int count = 10)
        {
            operation.WhereStatements.Add(operation.WhereStatements.Any()
                ? $"AND ({statement.Compile()})"
                : $"{statement.Compile()}");

            return operation.SelectManyAsync();
        }
    }
}