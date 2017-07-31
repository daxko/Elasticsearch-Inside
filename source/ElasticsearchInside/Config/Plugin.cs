﻿namespace Daxko.ElasticsearchInside.Config
{
    /// <summary>
    /// Defines properties needed to install a plugin.
    /// If URL is specified the plugin will be installed via:
    /// bin/plugin.bat install [Name] -url [Url]
    /// 
    /// Otherwise the plugin will be installed via:
    /// bin/plugin.bat install [Name]
    /// </summary>
    public class Plugin
    {
        /// <summary>
        /// Required.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Optional.
        /// </summary>
        public string Url { get; set; }

        public Plugin(string Name, string Url = null)
        {
            this.Name = Name;
            this.Url = Url;
        }

        internal string GetInstallCommand()
        {
            if (Url != null)
            {
                return string.Format("install \"{0}\"", Url);
            }
            return string.Format("install \"{0}\"", Name);
        }
    }
}
