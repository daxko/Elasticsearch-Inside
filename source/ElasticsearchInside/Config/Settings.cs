using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Daxko.ElasticsearchInside.Config
{
    internal class Settings : ISettings
    {
        private static readonly Random Random = new Random();
        internal readonly DirectoryInfo RootFolder;

        public Settings()
        {
            Plugins = new List<Plugin>();
            Logger = message => Trace.WriteLine(message);
            ElasticsearchVersion = ReadVersion();
            LoggingConfig = new List<string>();
            JVMParameters = new List<string>();
            ElasticsearchParameters = new Dictionary<string, string>();
            RootFolder = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N")));
        }

        public DirectoryInfo ElasticsearchHomePath
        {
            get { return new DirectoryInfo(Path.Combine(RootFolder.FullName, "es")); }
        }

        public DirectoryInfo JvmPath
        {
            get { return new DirectoryInfo(Path.Combine(RootFolder.FullName, "jre")); }
        }

        public IDictionary<string, string> ElasticsearchParameters { get; set; }
        public IList<string> JVMParameters { get; set; }
        public IList<string> LoggingConfig { get; set; }
        public string ElasticsearchVersion { get; set; }
        
        private static string ReadVersion()
        {
            var parts = Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion.Split('.', '-');
            return string.Format("{0}.{1}.{2}", parts[0], parts[1], parts[2]);
        }

        public string BuildCommandline()
        {
            return string.Format(
                "{0} -Des.path.home=\"{1}\" -cp \"lib/elasticsearch-{2}.jar;lib/*\" \"org.elasticsearch.bootstrap.Elasticsearch\"",
                string.Join(" ", JVMParameters), ElasticsearchHomePath.FullName, ElasticsearchVersion);
        }

        public static async Task<Settings> LoadDefault(CancellationToken cancellationToken = default(CancellationToken))
        {
            var port = Random.Next(49152, 65535 + 1);

            var settings = new Settings
            {
                JVMParameters = await ReadJVMDefaults(cancellationToken)
            };

            settings.SetPort(port);
            settings.SetClustername(string.Format("cluster-es-{0}", port));
            settings.SetNodename(string.Format("node-es-{0}", port));

            settings.LoggingConfig.Add("logger.zen.name = org.elasticsearch.discovery.zen.UnicastZenPing");
            settings.LoggingConfig.Add("logger.zen.level = error");
            settings.LoggingConfig.Add("logger.zen2.name = org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing");
            settings.LoggingConfig.Add("logger.zen2.level = error");

            return settings;

        }

        private static async Task<IList<string>> ReadJVMDefaults(CancellationToken cancellationToken = default(CancellationToken))
        {
            IList<string> result = new List<string>();

            using (var stream = typeof(ISettings).Assembly.GetManifestResourceStream(typeof(ISettings), "jvm.options"))
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream && !cancellationToken.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync();

                    if (line == null)
                        continue;

                    if (line.StartsWith("#"))
                        continue;

                    result.Add(line);
                }
            }

            return result;
        }

        internal async Task WriteSettings()
        {
            await WriteLoggingConfig();
            await WriteYaml();
        }

        internal async Task WriteYaml()
        {
            var configDir = new DirectoryInfo(Path.Combine(ElasticsearchHomePath.FullName, "config"));
            if (!configDir.Exists)
                configDir.Create();

            var file = new FileInfo(Path.Combine(configDir.FullName, @"elasticsearch.yml"));
            if (file.Exists)
                file.Delete();

            using (var fileStream = file.OpenWrite())
            using (var writer = new StreamWriter(fileStream))
                foreach (var elasticsearchParameter in ElasticsearchParameters)
                    await writer.WriteLineAsync(string.Format("{0}: {1}", elasticsearchParameter.Key,
                        elasticsearchParameter.Value));
        }

        internal async Task WriteLoggingConfig()
        {
            var configDir = new DirectoryInfo(Path.Combine(ElasticsearchHomePath.FullName, "config"));
            if (!configDir.Exists)
                configDir.Create();

            var file = new FileInfo(Path.Combine(configDir.FullName, @"log4j2.properties"));

            using (var fileStream = file.Open(FileMode.Append, FileAccess.Write))
            using (var writer = new StreamWriter(fileStream))
                foreach (var logsetting in LoggingConfig)
                    await writer.WriteLineAsync(logsetting);
        }

        public ISettings EnableLogging(bool enable = true)
        {
            this.LoggingEnabled = enable;
            return this;
        }

        public bool LoggingEnabled { get; set; }

        public Action<string> Logger { get; private set; }

        public IList<Plugin> Plugins { get; set; }

        public ISettings LogTo(Action<string> logger)
        {
            this.Logger = logger;
            return this;
        }

        public ISettings AddPlugin(Plugin plugin)
        {
            Plugins.Add(plugin);
            return this;
        }

        public void Dispose()
        {
            RootFolder.Delete(true);
        }
    }
}
