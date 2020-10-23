using System;

namespace BigQuery.Console.Domain
{
    public class Geo
    {
        public Guid Id { get; set; }
        
        public Guid UserId { get; set; }

        public float Latitude { get; set; }
        
        public float Longitude { get; set; }
        
        public string RawLocation { get; set; }
    }
}