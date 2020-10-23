using BigQuery.QueryBuilder;
using BigQuery.QueryBuilder.Infrastructure;

namespace BigQuery.Console.Domain
{
    public class UserTrackerRepository : BigQueryRepository
    {
        public UserTrackerRepository() : base(new ConnectionDetails
        {
            DatasetId = "dataset0",
            ProjectId = "bigquery-playground-290419"
        })
        {
        }
        
        public QueryableAsyncWrapper<Device> Devices { get; set; }
        
        public QueryableAsyncWrapper<Geo> Geos { get; set; }
        
        public QueryableAsyncWrapper<User> Users { get; set; }
    }
}