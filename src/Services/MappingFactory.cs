/*
 * Authors:
 *   喻星(Xing Yu) <491718907@qq.com>
 *
 * Copyright (C) 2015 Automao Network Co., Ltd. <http://www.zongsoft.com>
 *
 * This file is part of Automao.Data.
 *
 * Automao.Data is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * Automao.Data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Automao.Data; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.IO;

using Automao.Data.Options.Configuration;

namespace Automao.Data.Services
{
    public class MappingFactory : IMappingFactory
    {
        #region 构造函数
        #endregion

        #region 属性
        public MappingFiles MappingFiles { get; set; }
        #endregion

        #region 方法
        public Dictionary<string, string> GetMappingContext(string mappingFileName)
        {
            if (string.IsNullOrEmpty(mappingFileName))
                mappingFileName = "*";

            if (MappingFiles == null || MappingFiles.Count == 0)
                throw new ArgumentNullException("MappingFiles");

            var paths = MappingFiles == null ? null : MappingFiles.Cast<MappingFile>().Where(p => !string.IsNullOrEmpty(p.Path))
                .Select(p => ParsePath(p.Path)).ToList();

            if (paths == null || paths.Count == 0)
                paths = new List<string> { AppDomain.CurrentDomain.BaseDirectory };

            return GetMappingContext(paths.ToArray(), mappingFileName);
        }

        public Dictionary<string, string> GetMappingContext(string[] paths, string mappingFileName)
        {
            Console.WriteLine("[{0}]", string.Join("\r\n", paths));

            var result = new Dictionary<string, string>();
            foreach (var path in paths)
            {
                if (result.ContainsKey(path))
                    continue;

                if (path.StartsWith("http://"))
                {
                    var wc = new System.Net.WebClient();
                    var buffer = wc.DownloadData(path);
                    var text = Encoding.UTF8.GetString(buffer);
                    result.Add(path, text);
                }
                else
                {
                    var di = new DirectoryInfo(path);
                    if (di.Exists)
                    {
                        var files = System.IO.Directory.GetFiles(path, mappingFileName + ".mapping", System.IO.SearchOption.AllDirectories);

                        var temp = GetMappingContext(files, mappingFileName);
                        foreach (var item in temp)
                        {
                            result.Add(item.Key, item.Value);
                        }
                    }
                    else
                    {
                        var fi = new FileInfo(path);
                        if (fi.Exists)
                        {
                            using (var sr = fi.OpenText())
                            {
                                var text = sr.ReadToEnd();
                                result.Add(path, text);
                            }
                        }
                    }
                }
            }
            return result;
        }

        private string ParsePath(string path)
        {
            if (!path.Contains("..\\"))
                return path;

            var index = path.IndexOf("..\\");
            var directory = path.Substring(0, index);
            if (string.IsNullOrEmpty(directory))
                directory = Path.GetDirectoryName(this.GetType().Assembly.Location);
            directory = Directory.GetParent(directory).FullName;

            path = Path.Combine(directory, path.Substring(index + 3));
            return ParsePath(path);
        }
        #endregion
    }
}
