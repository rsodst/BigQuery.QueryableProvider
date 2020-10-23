using BigQuery.QueryBuilder;
using BigQuery.QueryBuilder.Infrastructure;

namespace BigQuery.Integration.Tests.Domain
{
    public class UsersRepository : BigQueryRepository
    {
        public UsersRepository(ConnectionDetails connectionDetails) : base(connectionDetails)
        {
        }

        public QueryableAsyncWrapper<UserDetail> UserDetails { get; set; }
    }

}