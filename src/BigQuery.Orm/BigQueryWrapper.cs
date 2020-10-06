using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.Orm
{
    public class BigQueryWrapper<TEntity> 
        where TEntity:new()
    {
        private readonly BigQueryClient _client;
        private readonly BigQueryConnectionOptions _options;

        public BigQueryWrapper(BigQueryConnectionOptions options)
        {
            _options = options;
            _client = BigQueryClient.Create(options.ProjectId, GoogleCredential.FromFile("secret.json"));
        }

        private static string TableName => typeof(TEntity).Name.ToLower();

        public void Insert<TInsertEntity>(TInsertEntity entity)
        {
            var tableReference = new TableReference
            {
                DatasetId = _options.DatasetId,
                ProjectId = _options.ProjectId,
                TableId = TableName
            };

            var bigQueryInsertRow = new BigQueryInsertRow();

            foreach (var property in ReflectionExtensions
                .GetProperties<TEntity>()
                .Where(p=> p.GetValue(entity) != null))
            {
                ThrowExceptionIfSystemClass(property);

                bigQueryInsertRow
                    .Add(property.Name, property.GetValue(entity));
            }

            _client.InsertRow(tableReference, bigQueryInsertRow);
        }

        public SimpleSelectQueryContext<TEntity> Select(params Expression<Func<TEntity, object>>[] properties)
        {
            var selectContext = new SimpleSelectQueryContext<TEntity>($"{_options.ProjectId}.{_options.DatasetId}.{TableName}",
                properties.Select(ReflectionExtensions.GetPropertyName))
            {
                ExecuteSql = ExecuteSql
            };

            return selectContext;
        }
        
        public SimpleSelectQueryContext<TEntity> Select()
        {
            var selectContext = new SimpleSelectQueryContext<TEntity>($"{_options.ProjectId}.{_options.DatasetId}.{TableName}")
            {
                ExecuteSql = ExecuteSql
            };

            return selectContext;
        }
        private IEnumerable<TEntity> ExecuteSql(string sql)
        {
            var job = _client.CreateQueryJob(sql, parameters: null,
                options: new QueryOptions() {UseQueryCache = false});

            job.PollUntilCompleted();

            var result = _client.GetQueryResults(job.Reference);

            var props = typeof(TEntity)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance);

            var entities = new List<TEntity>();

            foreach (var row in result)
            {
                var entity = new TEntity();
                        
                foreach (var field in row.Schema.Fields)
                {
                    var prop = typeof(TEntity).GetProperty(field.Name);
                            
                    prop?.SetValue(entity, row[field.Name]);
                }
                        
                entities.Add(entity);
            }

            return entities;
        }
        
        private static void ThrowExceptionIfSystemClass(PropertyInfo propertyInfo)
        {
            if (propertyInfo.GetType().IsClass
                && !new[] {"System.String", "System.Guid"}
                    .Contains(propertyInfo.PropertyType.FullName))
            {
                throw new NotImplementedException("Not implemented for Record Type");
            }

        }
    }
}