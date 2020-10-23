using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using BigQuery.QueryBuilder.Infrastructure;
using BigQuery.QueryBuilder.Operations;
using BigQuery.QueryBuilder.Utils;
using Google;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.QueryBuilder
{
    public class QueryableAsyncWrapper<TEntity> : IQueryableWrapper, IDDLWrapper
    {
        private readonly BigQueryContext _context;

        public QueryableAsyncWrapper(ConnectionDetails connectionDetails)
        {
            _context = new BigQueryContext(connectionDetails);
        }

        public SelectOperation<TEntity> Select(params Expression<Func<TEntity, object>>[] selectors)
        {
            return new SelectOperation<TEntity>(_context, selectors);
        }

        public Task Delete(GenericStatement whereStatement)
        {
            return new DeleteOperation<TEntity>(whereStatement).DeleteAsync();
        }

        public Task<BigQueryInsertResults> Insert(params TEntity[] entity)
        {
            return new InsertOperation<TEntity>(_context, entity).InsertAsync();
        }

        public UpdateOperation<TEntity> Update(GenericStatement whereStatement)
        {
            return new UpdateOperation<TEntity>(whereStatement);
        }

        public async Task<bool> IsExists()
        {
            try
            {
                await _context
                    .BigQueryClient.GetTableAsync(_context.CreateTableReference<TEntity>());

                return true;
            }
            catch (GoogleApiException ex)
            {
                return !(ex.Error.Code == 404);
            }
        }

        public async Task Drop()
        {
            if (!await IsExists())
            {
                return;
            }
            
            try
            {
                var table = await _context
                    .BigQueryClient.GetTableAsync(_context.CreateTableReference<TEntity>());

                await table.DeleteAsync();
            }
            catch (GoogleApiException ex)
            {
            }
        }

        public async Task Create()
        {
            if (await IsExists())
            {
                return;
            }

            var schema = new TableSchema
            {
                Fields = new List<TableFieldSchema>()
            };

            ReflectionUtils
                .GetProperties<TEntity>()
                .ToList()
                .ForEach(p =>
                {
                    schema.Fields.Add(new TableFieldSchema
                    {
                        Description = p.Name,
                        Name = p.Name,
                        Type = BigQueryTypeMapper.MapTypeToBigQuery[p.PropertyType]()
                    });
                });

            var table = new Table
            {
                Kind = _context.CreateTableName<TEntity>(),
                Description = _context.CreateTableName<TEntity>(),
                TableReference = _context.CreateTableReference<TEntity>(),
                Schema = schema
            };

            await _context.BigQueryClient
                .CreateTableAsync(_context.CreateTableReference<TEntity>(), table);
        }
    }
}