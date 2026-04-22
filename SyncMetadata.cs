namespace YBDriverBugPoc
{
    internal class SyncMetadata
    {
        public int Id { get; set; }
        public DateTimeOffset? LastUpdateDateTime { get; set; }
        public long? LastUpdateID { get; set; }
        public DateTimeOffset? LastDeleteDateTime { get; set; }
    }
}