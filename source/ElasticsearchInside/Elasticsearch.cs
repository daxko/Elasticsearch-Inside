﻿using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Daxko.ElasticsearchInside.Config;
using Daxko.ElasticsearchInside.Executables;
using Daxko.ElasticsearchInside.Utilities;
using Daxko.ElasticsearchInside.Utilities.Archive;
using LZ4PCL;
using CompressionMode = LZ4PCL.CompressionMode;

namespace Daxko.ElasticsearchInside
{
    /// <summary>
    /// Starts a elasticsearch instance in the background, use Ready() to wait for start to complete
    /// </summary>
    public class Elasticsearch : IDisposable
    {
        private bool _disposed;
        private readonly Stopwatch _stopwatch;
        private ProcessWrapper _processWrapper;
        private readonly Task _startupTask;
        private Settings _settings;

        static Elasticsearch()
        {
            AppDomain.CurrentDomain.AssemblyResolve += (s, e) =>
            {
                if (e.Name != "LZ4PCL, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")
                    return null;

                using (var memStream = new MemoryStream())
                {
                    using (var stream = typeof(Elasticsearch).Assembly.GetManifestResourceStream(typeof(RessourceTarget), "LZ4PCL.dll"))
                        stream.CopyTo(memStream);

                    return Assembly.Load(memStream.GetBuffer());
                }
            };
        }

        public Uri Url
        {
            get { return _settings.GetUrl(); }
        }

        public ISettings Settings
        {
            get { return _settings; }
        }

        public async Task<Elasticsearch> Ready()
        {
            await _startupTask;
            return this;
        }

        public Elasticsearch ReadySync()
        {
            return Ready().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private void Info(string message)
        {
            if (_settings == null || !_settings.LoggingEnabled)
                return;

            _settings.Logger(message);
        }

        public Elasticsearch(Func<ISettings, ISettings> configurationAction = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            _stopwatch = Stopwatch.StartNew();
            _startupTask = SetupAndStart(configurationAction, cancellationToken);
        }

        private async Task SetupAndStart(Func<ISettings, ISettings> configurationAction, CancellationToken cancellationToken = default(CancellationToken))
        {
            _settings = await Config.Settings.LoadDefault(cancellationToken).ConfigureAwait(false);
            if (configurationAction != null) configurationAction.Invoke(_settings);

            Info(string.Format("Starting Elasticsearch {0}", _settings.ElasticsearchVersion));
            
            await SetupEnvironment(cancellationToken).ConfigureAwait(false);
            Info(string.Format("Environment ready after {0} seconds", _stopwatch.Elapsed.TotalSeconds));
            await StartProcess(cancellationToken).ConfigureAwait(false);
            Info("Process started");
            await WaitForOk(cancellationToken).ConfigureAwait(false);
            Info("We got ok");
            await InstallPlugins(cancellationToken).ConfigureAwait(false);
            Info("Installed plugins");
        }

        private async Task InstallPlugins(CancellationToken cancellationToken = default(CancellationToken))
        {
            foreach (var plugin in _settings.Plugins)
            {
                Info(string.Format("Installing plugin {0}...", plugin.Name));
                using (var process = new ProcessWrapper(
                    new DirectoryInfo(Path.Combine(_settings.ElasticsearchHomePath.FullName, "bin")),
                    Path.Combine(_settings.ElasticsearchHomePath.FullName, "bin\\elasticsearch-plugin.bat"),
                    plugin.GetInstallCommand(),
                    Info,
                    startInfo =>
                    {
                        if (startInfo.EnvironmentVariables.ContainsKey("JAVA_HOME"))
                        {
                            Info("Removing old JAVA_HOME and replacing with bundled JRE.");
                            startInfo.EnvironmentVariables.Remove("JAVA_HOME");
                        }
                        startInfo.EnvironmentVariables.Add("JAVA_HOME", _settings.JvmPath.FullName);
                    }
                ))
                {
                    await process.Start(cancellationToken).ConfigureAwait(false);
                    Info(string.Format("Waiting for plugin {0} install...", plugin.Name));
                    process.WaitForExit();
                }
                Info(string.Format("Plugin {0} installed.", plugin.Name));
                await Restart().ConfigureAwait(false);
            }
        }

        private async Task SetupEnvironment(CancellationToken cancellationToken = default(CancellationToken))
        {
            var jreTask = Task.Run(() => ExtractEmbeddedLz4Stream("jre.lz4", _settings.JvmPath, cancellationToken), cancellationToken);
            var esTask = Task.Run(() => ExtractEmbeddedLz4Stream("elasticsearch.lz4", _settings.ElasticsearchHomePath, cancellationToken), cancellationToken)
                .ContinueWith(_ => _settings.WriteSettings(), cancellationToken);

            await Task.WhenAll(jreTask, esTask).ConfigureAwait(false);
        }


        private async Task WaitForOk(CancellationToken cancellationToken = default(CancellationToken))
        {
            var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var linked = CancellationTokenSource.CreateLinkedTokenSource(timeoutSource.Token, cancellationToken);
            
            var statusUrl = new UriBuilder(_settings.GetUrl())
            {
                Path = "_cluster/health",
                Query = "wait_for_status=yellow"
            }.Uri;

            using (var client = new HttpClient())
            {
                var statusCode = (HttpStatusCode)0;
                do
                {
                    try
                    {
                        var response = await client.GetAsync(statusUrl, linked.Token);
                        statusCode = response.StatusCode;
                    }
                    catch (HttpRequestException) { }
                    catch (TaskCanceledException ex) {
                        throw new TimeoutWaitingForElasticsearchStatusException(ex); 
                    }
                    await Task.Delay(100, linked.Token).ConfigureAwait(false);

                } while (statusCode != HttpStatusCode.OK && !linked.IsCancellationRequested);
            }
            
            _stopwatch.Stop();
            Info(string.Format("Started in {0} seconds", _stopwatch.Elapsed.TotalSeconds));
        }

        private async Task StartProcess(CancellationToken cancellationToken = default(CancellationToken))
        {
            var args = _settings.BuildCommandline();

            _processWrapper = new ProcessWrapper(_settings.ElasticsearchHomePath, Path.Combine(_settings.JvmPath.FullName, "bin/java.exe"), args, Info);
            await _processWrapper.Start(cancellationToken).ConfigureAwait(false);
        }
        
        public async Task Restart()
        {
            await _processWrapper.Restart().ConfigureAwait(false);
            await StartProcess().ConfigureAwait(false);
            await WaitForOk().ConfigureAwait(false);
        }

        private async Task ExtractEmbeddedLz4Stream(string name, DirectoryInfo destination, CancellationToken cancellationToken = default(CancellationToken))
        {
            var started = Stopwatch.StartNew();

            using (var stream = GetType().Assembly.GetManifestResourceStream(typeof(RessourceTarget), name))
            using (var decompresStream = new LZ4Stream(stream, CompressionMode.Decompress))
            using (var archiveReader = new ArchiveReader(decompresStream))
                await archiveReader.ExtractToDirectory(destination, cancellationToken).ConfigureAwait(false);
           
            Info(string.Format("Extracted {0} in {1:#0.##} seconds", name.Split('.')[0], started.Elapsed.TotalSeconds));
        }


        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            try
            {
                _processWrapper.Dispose();
                _settings.Dispose();
            }
            catch (Exception ex)
            {
                Info(ex.ToString());
            }
            _disposed = true;

        }

        ~Elasticsearch()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
