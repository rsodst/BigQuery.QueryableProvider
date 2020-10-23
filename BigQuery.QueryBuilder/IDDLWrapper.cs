using System.Threading.Tasks;

namespace BigQuery.QueryBuilder
{
    public interface IDDLWrapper
    {
        Task Drop();
        Task Create();
    }
}