using System;

namespace Daxko.ElasticsearchInside
{
    public class TimeoutWaitingForElasticsearchStatusException : Exception
    {
        public TimeoutWaitingForElasticsearchStatusException(Exception ex) : base("Timeout waiting for Elasticsearch status", ex)
        {
        }
    }
}