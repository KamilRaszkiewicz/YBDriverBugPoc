using System.Data.Common;
using YBNpgsql;

namespace YBDriverBugPoc
{
    internal class Program
    {
        /* SQL SCRIPT FOR CREATING IMILAR DB STRUCTURE
         * 
       
            CREATE TABLE IF NOT EXISTS public.syncmetadata
            (
                id integer NOT NULL,
                name character varying(255) COLLATE pg_catalog."default" NOT NULL,
                lastupdatedatetime timestamp(6) without time zone,
                lastupdateid bigint,
                lastdeletedatetime timestamp(6) without time zone,
                CONSTRAINT syncmetadata_pkey PRIMARY KEY (id)
            );

            insert into syncmetadata(id, name)
            values
            (0, '0'),
            (1, '1'),
            (2, '2'),
            (3, '3'),
            (4, '4'),
            (5, '5'),
            (6, '6'),
            (7, '7'),
            (8, '8'),
            (9, '9');

            CREATE TABLE IF NOT EXISTS public.table0
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table0_pkey PRIMARY KEY (id)
            );

            CREATE TABLE IF NOT EXISTS public.table1
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table1_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table2
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table2_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table3
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table3_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table4
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table4_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table5
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table5_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table6
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table6_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table7
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table7_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table8
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table8_pkey PRIMARY KEY (id)
            );

        
            CREATE TABLE IF NOT EXISTS public.table9
            (
                id integer NOT NULL,
                value character varying(255) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT table9_pkey PRIMARY KEY (id)
            );
         */


        static async Task Main(string[] args)
        {
            var dataSource = NpgsqlDataSource.Create("Host=x.x.x.x,y.y.y.y,z.z.z.z;Port=5432;Username=postgres;Password=postgres;Database=tidemms;Load Balance Hosts=true;");

            var tasks = Enumerable.Range(0, 10).Select( async (x) => await ExecuteForIDAsync(x, dataSource)).ToList();

            await Task.WhenAll(tasks);
        }

        private static async Task ExecuteForIDAsync(int id, NpgsqlDataSource datasource)
        {
            while (true)
            {
                int delay = 1000;

                try
                {
                    var result = await GetSyncMetadataAsync(id, datasource);

                    var count = await UpdateTable(id, datasource);

                    if (count > 0)
                        delay = 50; //actually it will always be this in this case

                    await SetSyncMetadataAsync(id, DateTime.UtcNow, 1, DateTime.UtcNow, datasource);

                    Console.Write(".");
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"\nException caught! Exception: {ex}");
                }

                await Task.Delay(delay);
            }
        }

        private static async Task<SyncMetadata> GetSyncMetadataAsync(int entityType, NpgsqlDataSource dataSource)
        {
            await using var connection = await dataSource.OpenConnectionAsync();

            using var command = new NpgsqlCommand("""
                SELECT id, name, lastupdatedatetime, lastupdateid, lastdeletedatetime
                FROM public.syncmetadata
                WHERE id = @id
            """, connection);

            command.Parameters.AddWithValue("id", (int)entityType);

            await using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                return new SyncMetadata
                {
                    Id = reader.GetInt32(reader.GetOrdinal("id")),
                    LastUpdateDateTime = reader.IsDBNull(reader.GetOrdinal("lastupdatedatetime"))
                        ? null
                        : reader.GetDateTime(reader.GetOrdinal("lastupdatedatetime")),
                    LastUpdateID = reader.IsDBNull(reader.GetOrdinal("lastupdateid"))
                        ? null
                        : reader.GetInt64(reader.GetOrdinal("lastupdateid")),
                    LastDeleteDateTime = reader.IsDBNull(reader.GetOrdinal("lastdeletedatetime"))
                        ? null
                        : reader.GetDateTime(reader.GetOrdinal("lastdeletedatetime")),
                };
            }

            throw new Exception($"Could not find syncmetdata entry for entityType '{entityType}'");
        }

        public static async Task SetSyncMetadataAsync(int entityType, DateTime? lastUpdateDateTime, long? lastUpdateId, DateTime? lastDeleteDateTime, NpgsqlDataSource dataSource)
        {
            await using var connection = await dataSource.OpenConnectionAsync();

            using var command = new NpgsqlCommand("""
                UPDATE public.syncmetadata
                SET lastupdatedatetime = coalesce(@lastupdatedatetime, lastupdatedatetime),
                    lastupdateid       = @lastupdateid,
                    lastdeletedatetime = coalesce(@lastdeletedatetime, lastdeletedatetime)
                WHERE id = @id
            """, connection);

            command.Parameters.AddWithValue("id", (int)entityType);

            command.Parameters.AddWithValue("lastupdatedatetime",
                (object?)lastUpdateDateTime ?? DBNull.Value);

            command.Parameters.AddWithValue("lastupdateid",
                (object?)lastUpdateId ?? DBNull.Value);

            command.Parameters.AddWithValue("lastdeletedatetime",
                (object?)lastDeleteDateTime ?? DBNull.Value);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task<int> UpdateTable(int entityType, NpgsqlDataSource dataSource)
        {
            await using var connection = await dataSource.OpenConnectionAsync();

            var ids = Enumerable.Range(1, 2_500).ToArray();

            var names = ids.Select(i => (Random.Shared.Next() % 10 == 0) ? i.ToString() : (i+10).ToString()).ToArray();

            using var command = new NpgsqlCommand($"""
                INSERT INTO public.table{entityType} (id, value)
                SELECT * FROM unnest(@ids, @names) AS u(id, value)
                ON CONFLICT (id) DO UPDATE
                SET value = EXCLUDED.value;
            """, connection);

            command.Parameters.AddWithValue("ids", ids);
            command.Parameters.AddWithValue("names", names);

            return await command.ExecuteNonQueryAsync();
        }
    }
}
