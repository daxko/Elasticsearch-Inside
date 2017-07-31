﻿using System;
using System.IO;
using System.Threading.Tasks;
using Daxko.ElasticsearchInside.Config;
using NUnit.Framework;

namespace Daxko.ElasticsearchInside.Tests.Config
{
    [TestFixture]
    public class SettingsTests
    {
        [Test]
        public async Task Can_write_yaml()
        {
            using (var settings = new Settings())
            {
                ////Arrange
                settings.SetClustername("test");

                ////Act
                await settings.WriteYaml();

                ////Assert
                var yamlFile = new FileInfo(Path.Combine(settings.ElasticsearchHomePath.FullName, "config/elasticsearch.yml"));
                string result;
                using (var reader = yamlFile.OpenText())
                    result = reader.ReadToEnd();

                var lines = result.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                Assert.That(lines.Length, Is.EqualTo(1));
                Assert.That(lines[0], Is.EqualTo("cluster.name: test"));
            }
        }
    }
}
