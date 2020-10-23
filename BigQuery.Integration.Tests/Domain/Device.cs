using System;

namespace BigQuery.Console.Domain
{
    public class Device
    {
        public Guid Id { get; set; }
        
        public Guid UserId { get; set; }
        
        public int Type { get; set; }
        
        public float DispersionCoef { get; set; }
    }
}