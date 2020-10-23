using System;
using System.Linq;
using System.Threading.Tasks;
using BigQuery.QueryBuilder.Infrastructure;

namespace BigQuery.QueryBuilder
{
    public abstract class BigQueryRepository
    {
        protected BigQueryRepository(ConnectionDetails connectionDetails)
        {
            GetType().GetProperties()
                .Where(p => typeof(IQueryableWrapper).IsAssignableFrom(p.PropertyType))
                .ToList()
                .ForEach(p =>
                {
                    var type = Activator.CreateInstance(p.PropertyType, connectionDetails);
                    p.SetValue(this, type);
                });
        }

        public async Task EnsureTablesCreated()
        {
            var ddlWrappers = GetType().GetProperties()
                .Where(p => typeof(IDDLWrapper).IsAssignableFrom(p.PropertyType))
                .Select(p => p.GetValue(this) as IDDLWrapper)
                .ToList();

            foreach (var wrapper in ddlWrappers)
            {
                await wrapper.Create();    
            }
        }
    }
}