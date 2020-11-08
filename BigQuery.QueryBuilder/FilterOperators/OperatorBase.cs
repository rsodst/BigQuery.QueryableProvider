namespace BigQuery.QueryBuilder
{
    public abstract class OperatorBase
    {
        public string Filter;

        private void Composite(OperatorBase y, string condition)
        {
            Filter = ($"({Filter} {condition} {y.Filter})");
        } 
        
        public static OperatorBase operator |(OperatorBase x, OperatorBase y)
        {
            x.Composite(y, "OR");

            return x;
        }
        
        public static OperatorBase operator &(OperatorBase x, OperatorBase y)
        {
            x.Composite(y, "AND");

            return x;
        }
    }
}