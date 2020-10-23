using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using BigQuery.QueryBuilder.Utils;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.QueryBuilder.Operations
{
    public class InsertOperation<TEntity>
    {
        private readonly BigQueryContext _context;
        private readonly TEntity[] _entities;

        public InsertOperation(BigQueryContext context, params TEntity[] entities)
        {
            _context = context;
            _entities = entities;
        }

        public Task<BigQueryInsertResults> InsertAsync()
        {
            return _entities.Length == 1 ? InsertAsync(_entities.First()) : InsertManyAsync(_entities);
        }

        // private

        private Task<BigQueryInsertResults> InsertAsync(TEntity entity)
        {
            var bigQueryInsertRow = new BigQueryInsertRow();

            foreach (var property in ReflectionUtils
                .GetProperties<TEntity>()
                .Where(p => p.GetValue(entity) != null))
            {
                var (name, value) = FormatRow(property, entity);
                bigQueryInsertRow.Add(name, value);
            }

            return _context.BigQueryClient
               .InsertRowAsync(_context.CreateTableReference<TEntity>(), bigQueryInsertRow);
        }

        private Task<BigQueryInsertResults> InsertManyAsync(IEnumerable<TEntity> entities)
        {
            var bigQueryInsertRows = new List<BigQueryInsertRow>();

            foreach (var entity in entities)
            {
                var insertedRow = new BigQueryInsertRow();

                foreach (var property in ReflectionUtils
                    .GetProperties<TEntity>()
                    .Where(p => p.GetValue(entity) != null))
                {
                    var (name, value) = FormatRow(property, entity);
                    insertedRow.Add(name, value);
                }

                bigQueryInsertRows.Add(insertedRow);
            }

            return _context.BigQueryClient
                .InsertRowsAsync(_context.CreateTableReference<TEntity>(), bigQueryInsertRows);
        }

        private static (string name, string value) FormatRow(PropertyInfo property, TEntity entity)
        {
            return (property.Name,
                    $"{BigQueryTypeMapper.MapToBigQuery[property.PropertyType](property.GetValue(entity))}");
        }

        private static void ThrowExceptionIfSystemClass(PropertyInfo propertyInfo)
        {
            if (propertyInfo.GetType().IsClass
                && !new[] {"System.String", "System.Guid", "System.DateTime"}
                    .Contains(propertyInfo.PropertyType.FullName))
            {
                throw new NotImplementedException("Not implemented for Record Type");
            }
        }
    }
}