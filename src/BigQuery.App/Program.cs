using System;
using BigQuery.Orm;

namespace BigQuery.App
{
    public class NetstedTable
    {
        public string FirstName { get; set; }

        public string LastName { get; set; }
    }

    public class Table1
    {
        public string Id { get; set; }
        public string Data { get; set; }

        public NetstedTable NestedDetails { get; set; }
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            // var cond1 = BigQueryPredicate<Table1>.Equal(p => p.Id, Guid.Empty);
            // var cond2 = BigQueryPredicate<Table1>.Equal(p => p.Data, "456");
            //
            // var t = BigQueryPredicate.And(cond1, BigQueryPredicate
            //     .Or(cond1,BigQueryPredicate.And(BigQueryPredicate<Table1>.Equal(p=> p.Data,"428"), 
            //         BigQueryPredicate.Or(BigQueryPredicate<Table1>.Equal(p => p.Data, "456"), BigQueryPredicate<Table1>.Equal(p => p.Data, "456")))
            //     ));
            //
            // var c = t.Compile();



            var usersContext = new BigQueryWrapper<Table1>(new BigQueryConnectionOptions
            {
                ProjectId = "bigquery-playground-290419",
                DatasetId = "dataset0"
            });
            
            
            // usersContext.Insert(new Table1
            // {
            //     Id = $"{Guid.NewGuid()}",
            //     Data = "abc"
            // });

            // var result0 = usersContext.Select().Where(BigQueryPredicate<Table1>.Equal(p => p.Id, "123")).First();
            //
            // var result1 = usersContext.Select().Where(
            //     BigQueryPredicate.And(
            //         BigQueryPredicate<Table1>.Equal(p=>p.Data, "data"), 
            //     BigQueryPredicate<Table1>.Equal(p=>p.Data,"indata"))
            //     ).First();

            var result3 = usersContext
                .Select(p => p.Id, p => p.Data)
                .Where(BigQueryPredicates<Table1>.Equal(p => p.Id, "123"));
            
            var result2 = usersContext
                .Select(p=>p.Id, p=>p.Data)
                .Where(
                    BigQueryPredicate<Table1>.Or(
                        BigQueryPredicates<Table1>.Equal(p=>p.Data, "Jack"),
                        BigQueryPredicates<Table1>.Equal(p=>p.Data, "abc"))
            ).ToList();

            
            // var result = usersContext
            //     .Select(table1 => table1.Id)
            //     .Where(new BigQueryFilter<Table1>(p => p.Data, Condition.NotEqual(), "Some Data"))
            //     .And(new BigQueryFilter<Table1>(p => p.Data, Condition.NotEqual(), "Some Data 2"))
            //     .ToList();
        }
    }
}