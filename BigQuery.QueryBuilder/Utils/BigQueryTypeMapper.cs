using System;
using System.Collections.Generic;

namespace BigQuery.QueryBuilder.Utils
{
    public static class BigQueryTypeMapper
    {
        public static readonly Dictionary<Type, Func<object, object>> MapFromBigQuery =
            new Dictionary<Type, Func<object, object>>
            {
                [typeof(Guid)] = s => Guid.Parse($"{s}"),
                [typeof(int)] = s => int.Parse($"{s}"),
                [typeof(float)] = s => float.Parse($"{s}"),
                [typeof(double)] = s => double.Parse($"{s}"),
                [typeof(string)] = s => $"{s}",
                [typeof(DateTime)] = s => DateTime.Parse($"{s}")
            };
        
        public static readonly Dictionary<Type, Func<object, object>> MapToBigQuery =
            new Dictionary<Type, Func<object, object>>
            {
                [typeof(Guid)] = s => $"{s}",
                [typeof(int)] = s => $"{s}",
                [typeof(float)] = s => $"{s}".Replace(",","."), // nfi?
                [typeof(double)] = s => $"{s}",
                [typeof(string)] = s => $"{s}",
                [typeof(DateTime)] = s => $"{(DateTime)s:yyyy-M-dd hh:mm:ss}"
            };

        public static readonly Dictionary<Type, Func<string>> MapTypeToBigQuery = new Dictionary<Type, Func<string>>
        {
            [typeof(Guid)] = () => "STRING",
            [typeof(int)] = () => "INTEGER",
            [typeof(float)] = () => "FLOAT",
            [typeof(bool)] = () => "BOOLEAN",
            [typeof(DateTime)] = () => "TIMESTAMP",
            [typeof(string)] = () => "STRING"
        };
    }
}