using System;
using BigQuery.QueryBuilder.Infrastructure;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.QueryBuilder
{
    public class BigQueryContext : IDisposable
    {
        private readonly ConnectionDetails _connectionDetails;

        public readonly BigQueryClient BigQueryClient;

        public BigQueryContext(ConnectionDetails connectionDetails)
        {
            _connectionDetails = connectionDetails;
            
            BigQueryClient = BigQueryClient.Create(connectionDetails.ProjectId, GoogleCredential.FromFile("secret.json"));
        }
        
        public string CreateTableName<TEntity>() => $"{typeof(TEntity).Name}s"; 
        
        public TableReference CreateTableReference<TEntity>()
        {
            return new TableReference
            {
                DatasetId = _connectionDetails.DatasetId,
                ProjectId = _connectionDetails.ProjectId,
                TableId = CreateTableName<TEntity>()
            };
        }

        public DatasetReference CreateDatasetReference()
        {
            return new DatasetReference
            {
              DatasetId = _connectionDetails.DatasetId,
              ProjectId = _connectionDetails.ProjectId
            };
        }

        public string CreateFromStatement<TEntity>()
        {
            return $"{_connectionDetails.ProjectId}.{_connectionDetails.DatasetId}.{CreateTableName<TEntity>()}";                             
        }

        public void Dispose()
        {
            BigQueryClient?.Dispose();
        }
    }
}