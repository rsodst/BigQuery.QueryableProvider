using System;

namespace BigQuery.Console.Domain
{
    public class User
    {
        public Guid Id { get; set; }
        
        public DateTime CreatedOn { get; set; }
        public DateTime ModifiedOn { get; set; }
        
        public string Source { get; set; }
    }
}