using System;
using BigQuery.QueryBuilder;
using BigQuery.QueryBuilder.Infrastructure;

namespace BigQuery.Migrator
{
    public class Migration
    {
        public Guid Id { get; set; }
        
        public string Code { get; set; }
        
        public DateTime CreatedOn { get; set; }
    }

    public class MigrationsRepository : BigQueryRepository
    {
        public MigrationsRepository(ConnectionDetails connectionDetails) : base(connectionDetails)
        {
        }
        
        public QueryableAsyncWrapper<Migration> Migrations { get; set; }
    }
    
    public class MigrationsContext
    {
        
    }
}